from concurrent.futures import ThreadPoolExecutor
import json
import re
import requests
import os.path
import sys
from threading import Lock

# ---------------------------------------------------------------
# Load external settings files
# ---------------------------------------------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
_config_dir = os.path.join(_script_dir, "configuration")
os.makedirs(_config_dir, exist_ok=True)

def _load_json_settings(filename, description):
    path = os.path.join(_config_dir, filename)
    if not os.path.isfile(path):
        print(f"ERROR: {filename} not found at {path}")
        print(f"This file is required for {description}.")
        sys.exit(1)
    with open(path, 'r') as _f:
        return json.load(_f)

texture_settings  = _load_json_settings("texture_settings.json",  "texture type/resolution settings")
mesh_lod_settings = _load_json_settings("mesh_lod_settings.json", "mesh LOD settings")

# Derive enabled LOD indices and mesh mime type from mesh_lod_settings
enabled_lods      = mesh_lod_settings.get("lods", [0, 1, 2, 3, 4, 5])
mesh_mime_type    = mesh_lod_settings.get("mesh_mime_type", "application/x-fbx")
albedo_lods_flag  = mesh_lod_settings.get("albedo_lods", True)

print(f"-> Texture settings: {len([t for t in texture_settings['textures'] if t.get('enabled',True)])} types enabled")
print(f"-> Mesh LODs: {enabled_lods} | format: {mesh_mime_type}")

# ---------------------------------------------------------------
# 1. Set auth token, get cache, and init tracker variables
# Load auth token from authentication.txt
_auth_file = os.path.join(_config_dir, "authentication.txt")
if not os.path.isfile(_auth_file):
    print(f"ERROR: authentication.txt not found at {_auth_file}")
    print("Create authentication.txt in the same folder as this script with your Bearer token on the first non-comment line.")
    sys.exit(1)
with open(_auth_file, 'r') as _f:
    authToken = next((line.strip() for line in _f if line.strip() and not line.strip().startswith('#')), None)
if not authToken or authToken == 'YOUR_TOKEN_HERE':
    print("ERROR: No valid token found in authentication.txt. Paste your Bearer token in that file.")
    sys.exit(1)
print(f"-> Loaded auth token from authentication.txt ({authToken[:12]}...)")


# Asset types directory
ASSET_TYPES_DIR = os.path.join(_config_dir, "asset_types")
MISSING_TYPES_FILE = os.path.join(_config_dir, "missing_component_types.json")
DOWNLOAD_FAILED_FILE = os.path.join(ASSET_TYPES_DIR, "download_failed.json")

if os.path.isfile(os.path.join(_config_dir, "cache.txt")):
    with open(os.path.join(_config_dir, "cache.txt"), "r") as f:
        line = f.readline()
        if line == "":
            cache = []
        else:
            cache = line.split(",")
else:
    cache = []

num_to_download = 0
successful_downloads = 0
failed_downloads = 0
cache_lock = Lock()
counter_lock = Lock()
missing_types_lock = Lock()
failed_downloads_lock = Lock()

# Load or initialize missing types tracking
if os.path.isfile(MISSING_TYPES_FILE):
    with open(MISSING_TYPES_FILE, 'r') as f:
        try:
            missing_types_data = json.load(f)
        except:
            missing_types_data = {"discovered_types": [], "assets_with_missing_types": {}}
else:
    missing_types_data = {"discovered_types": [], "assets_with_missing_types": {}}

# Load or initialize failed downloads tracking
if os.path.isfile(DOWNLOAD_FAILED_FILE):
    with open(DOWNLOAD_FAILED_FILE, 'r') as f:
        try:
            failed_downloads_data = json.load(f)
            if "asset_ids" not in failed_downloads_data:
                failed_downloads_data["asset_ids"] = []
            if "count" not in failed_downloads_data:
                failed_downloads_data["count"] = 0
        except:
            failed_downloads_data = {"count": 0, "asset_ids": {}}
else:
    failed_downloads_data = {"count": 0, "asset_ids": {}}


def save_cache():
    with cache_lock:
        with open(os.path.join(_config_dir, "cache.txt"), "w") as f:
            f.write(",".join(cache))


def save_missing_types():
    with missing_types_lock:
        with open(MISSING_TYPES_FILE, 'w') as f:
            json.dump(missing_types_data, f, indent=2)


def _save_failed_downloads_unlocked():
    failed_downloads_data["count"] = len(failed_downloads_data["asset_ids"])
    with open(DOWNLOAD_FAILED_FILE, 'w') as f:
        json.dump(failed_downloads_data, f, indent=2)


def save_failed_downloads():
    with failed_downloads_lock:
        _save_failed_downloads_unlocked()


def track_failed_download(asset_id, reason="Unknown error"):
    with failed_downloads_lock:
        if "asset_ids" not in failed_downloads_data:
            failed_downloads_data["asset_ids"] = {}
        failed_downloads_data["asset_ids"][asset_id] = reason
        _save_failed_downloads_unlocked()


def load_failed_ids():
    if os.path.isfile(DOWNLOAD_FAILED_FILE):
        with open(DOWNLOAD_FAILED_FILE, 'r') as f:
            try:
                data = json.load(f)
                return set(data.get("asset_ids", {}).keys())
            except:
                return set()
    return set()


def remove_from_failed(asset_id):
    with failed_downloads_lock:
        if "asset_ids" not in failed_downloads_data:
            failed_downloads_data["asset_ids"] = {}
        if asset_id in failed_downloads_data["asset_ids"]:
            del failed_downloads_data["asset_ids"][asset_id]
            _save_failed_downloads_unlocked()


def track_missing_type(type_name, asset_id, mime_type):
    with missing_types_lock:
        type_entry = {
            "type": type_name,
            "mimeType": mime_type,
            "preferred_size": 4096
        }
        if not any(t["type"] == type_name for t in missing_types_data["discovered_types"]):
            missing_types_data["discovered_types"].append(type_entry)
            print(f"  --> NEW TYPE DISCOVERED: '{type_name}' (mimeType: {mime_type})")
        if asset_id not in missing_types_data["assets_with_missing_types"]:
            missing_types_data["assets_with_missing_types"][asset_id] = []
        if type_name not in missing_types_data["assets_with_missing_types"][asset_id]:
            missing_types_data["assets_with_missing_types"][asset_id].append(type_name)
        save_missing_types()


def list_available_asset_types():
    if not os.path.exists(ASSET_TYPES_DIR):
        return []
    json_files = [f for f in os.listdir(ASSET_TYPES_DIR) if f.endswith('.json')]
    asset_types = [f.replace('.json', '') for f in json_files]
    asset_types = [t for t in asset_types if t != "download_failed"]
    return asset_types


def getAcquiredIds(asset_type=None):
    if asset_type:
        json_file = os.path.join(ASSET_TYPES_DIR, f"{asset_type}.json")
        if os.path.exists(json_file):
            print(f"-> Loading {asset_type} assets from categorized JSON file...")
            with open(json_file, 'r') as f:
                data = json.load(f)
                print(f"-> Loaded {data['count']} {asset_type} assets from cache")
                return data['asset_ids']
        else:
            print(f"-> WARNING: Asset type file '{json_file}' not found.")
            print(f"-> Available asset types: {', '.join(list_available_asset_types())}")
            print(f"-> Falling back to fetching all assets from API...")

    heads = {
        "authorization": "Bearer " + authToken,
        "content-type": "application/json;charset=UTF-8",
        "Accept": "application/json",
    }
    response = requests.get("https://quixel.com/v1/assets/acquired", headers=heads)
    is_ok = response.ok
    response_data = response.json()
    if not is_ok:
        print(f"  --> ERROR: Error acquiring ids | [{response_data['statusCode']}] {response_data['message']}")
        sys.exit(0)
    all_asset_ids = [x["assetID"] for x in response_data]
    print(f"-> Found {len(all_asset_ids)} total assets from API")
    return all_asset_ids


def get_asset_metadata(asset_id):
    heads = {
        "authorization": "Bearer " + authToken,
        "content-type": "application/json;charset=UTF-8",
        "Accept": "application/json"
    }
    try:
        response = requests.get(f"https://quixel.com/v1/assets/{asset_id}", headers=heads, timeout=10)
        if response.ok:
            return response.json()
        else:
            print(f"  --> WARNING: Could not fetch metadata for {asset_id}")
            return None
    except requests.Timeout:
        print(f"  --> WARNING: Metadata fetch timed out for {asset_id}, using standard settings")
        return None
    except Exception as e:
        print(f"  --> WARNING: Error fetching metadata for {asset_id}: {e}")
        return None


def is_3d_asset(asset_metadata):
    """
    Detect if this is a 3D asset (not a plant/surface) based on having a
    'components' array with nested LOD-bearing formats.
    3D plants and surfaces use a flat 'maps' array instead.
    """
    if not asset_metadata:
        return False
    if 'components' not in asset_metadata:
        return False
    # A true 3D asset has formats with 'lodType' fields inside its components
    for component in asset_metadata.get('components', []):
        for uri_entry in component.get('uris', []):
            for res_entry in uri_entry.get('resolutions', []):
                for fmt in res_entry.get('formats', []):
                    if 'lodType' in fmt:
                        return True
    return False


def get_lod_count_from_metadata(asset_metadata):
    """
    Determine the number of LOD levels available in a 3D asset's components.
    Looks at the first component's first resolution to count distinct lod indices
    (formats without a lodType are the base/highpoly and are excluded).
    """
    for component in asset_metadata.get('components', []):
        for uri_entry in component.get('uris', []):
            for res_entry in uri_entry.get('resolutions', []):
                lod_indices = set()
                for fmt in res_entry.get('formats', []):
                    if fmt.get('lodType') == 'lod':
                        # Extract lod index from URI, e.g. "..._LOD0.jpg" -> 0
                        uri = fmt.get('uri', '')
                        match = re.search(r'_LOD(\d+)', uri, re.IGNORECASE)
                        if match:
                            lod_indices.add(int(match.group(1)))
                if lod_indices:
                    return sorted(lod_indices)
    return [0]


def build_3d_asset_components(asset_metadata, all_component_types, preferred_size):
    """
    For 3D assets (components array with LOD formats), build a component entry
    per texture type per LOD level, each with its own resolution and lod index.

    Returns a list of component dicts:
        {"type": ..., "resolution": ..., "mimeType": ..., "lod": <int>}
    """
    lod_indices = get_lod_count_from_metadata(asset_metadata)
    present_types = {c.get('type', '').lower() for c in asset_metadata.get('components', [])}
    components = []

    for comp_def in all_component_types:
        comp_type = comp_def["type"]
        mime_type = comp_def["mimeType"]

        if comp_type not in present_types:
            continue

        # Find the matching component in metadata
        matching_component = next(
            (c for c in asset_metadata['components'] if c.get('type') == comp_type),
            None
        )
        if not matching_component:
            continue

        for lod_idx in lod_indices:
            # Find the best resolution that actually has this LOD index available
            best_resolution = _find_resolution_for_3d_lod(
                matching_component, mime_type, preferred_size, lod_idx
            )
            if best_resolution:
                components.append({
                    "type": comp_type,
                    "resolution": best_resolution,
                    "mimeType": mime_type,
                    "lod": lod_idx
                })

    return components


def _find_resolution_for_3d_lod(component, mime_type, preferred_size, lod_idx):
    """
    For a single component from a 3D asset, find the best available resolution
    for a specific LOD index.

    Strategy:
    1. Try preferred mimeType with lodType=="lod" and matching LOD index in URI
    2. Try image/jpeg fallback with same lodType check
    3. Try any mimeType with lodType=="lod"
    4. FLAT FALLBACK: component has no lodType formats at all (e.g. bump, gloss on
       wooden chair). These are shared across all LODs — only include them once
       (lod_idx == 0) to avoid duplicates, picking best resolution normally.

    Returns (resolution, actual_mime_type) or (None, None).
    """
    mime_priority = [mime_type]
    if mime_type != "image/jpeg":
        mime_priority.append("image/jpeg")

    # Steps 1+2: LOD-bearing formats with preferred then fallback mimeType
    for try_mime in mime_priority:
        available_resolutions = []
        for uri_entry in component.get('uris', []):
            for res_entry in uri_entry.get('resolutions', []):
                resolution = res_entry.get('resolution', '')
                for fmt in res_entry.get('formats', []):
                    if fmt.get('mimeType') != try_mime:
                        continue
                    if fmt.get('lodType') != 'lod':
                        continue
                    uri = fmt.get('uri', '')
                    match = re.search(r'_LOD(\d+)', uri, re.IGNORECASE)
                    if match and int(match.group(1)) == lod_idx:
                        available_resolutions.append(resolution)
                        break
        if available_resolutions:
            return _pick_best_resolution(available_resolutions, preferred_size), try_mime

    # Step 3: any mimeType with lodType=="lod"
    for uri_entry in component.get('uris', []):
        for res_entry in uri_entry.get('resolutions', []):
            resolution = res_entry.get('resolution', '')
            for fmt in res_entry.get('formats', []):
                if fmt.get('lodType') != 'lod':
                    continue
                uri = fmt.get('uri', '')
                match = re.search(r'_LOD(\d+)', uri, re.IGNORECASE)
                if match and int(match.group(1)) == lod_idx:
                    return resolution, fmt.get('mimeType', mime_type)

    # Step 4: Flat fallback — component has no LOD-tagged formats at all.
    # Only emit for lod_idx==0 to avoid adding duplicate entries per LOD level.
    has_any_lod_format = any(
        fmt.get('lodType') == 'lod'
        for uri_entry in component.get('uris', [])
        for res_entry in uri_entry.get('resolutions', [])
        for fmt in res_entry.get('formats', [])
    )
    if not has_any_lod_format and lod_idx == 0:
        for try_mime in mime_priority:
            available_resolutions = []
            for uri_entry in component.get('uris', []):
                for res_entry in uri_entry.get('resolutions', []):
                    resolution = res_entry.get('resolution', '')
                    for fmt in res_entry.get('formats', []):
                        if fmt.get('mimeType') == try_mime:
                            available_resolutions.append(resolution)
                            break
            if available_resolutions:
                return _pick_best_resolution(available_resolutions, preferred_size), try_mime
        # Any mimeType flat
        for uri_entry in component.get('uris', []):
            for res_entry in uri_entry.get('resolutions', []):
                formats = res_entry.get('formats', [])
                if formats:
                    return res_entry.get('resolution', ''), formats[0].get('mimeType', mime_type)

    return None, None


def check_and_add_missing_types(asset_metadata, asset_id, component_types_list):
    if not asset_metadata:
        return component_types_list

    existing_types = {comp["type"] for comp in component_types_list}
    discovered = []

    if 'components' in asset_metadata:
        for component in asset_metadata['components']:
            comp_type = component.get('type', '').lower()
            if not comp_type:
                continue
            mime_type = 'image/jpeg'
            for uri_entry in component.get('uris', []):
                for res_entry in uri_entry.get('resolutions', []):
                    formats = res_entry.get('formats', [])
                    if formats:
                        mime_type = formats[0].get('mimeType', 'image/jpeg')
                        break
                break
            discovered.append((comp_type, mime_type))

    elif 'maps' in asset_metadata:
        seen = set()
        for map_entry in asset_metadata['maps']:
            comp_type = map_entry.get('type', '').lower()
            mime_type = map_entry.get('mimeType', 'image/jpeg')
            if comp_type and comp_type not in seen:
                seen.add(comp_type)
                discovered.append((comp_type, mime_type))

    for comp_type, mime_type in discovered:
        if comp_type not in existing_types:
            new_component = {
                "type": comp_type,
                "mimeType": mime_type,
                "preferred_size": 4096
            }
            component_types_list.append(new_component)
            existing_types.add(comp_type)
            track_missing_type(comp_type, asset_id, mime_type)

    return component_types_list


def find_resolution_for_component(asset_metadata, component_type, mime_type, preferred_size):
    """
    For flat-map assets (surfaces, 3D plants): find best resolution from maps array.
    For 3D assets this is NOT used — use build_3d_asset_components instead.
    """
    if not asset_metadata:
        return None

    if 'components' in asset_metadata:
        # Non-LOD components path (shouldn't normally be called for 3D assets,
        # but handles components that have no lodType formats)
        matching_component = next(
            (c for c in asset_metadata['components'] if c.get('type') == component_type),
            None
        )
        if matching_component:
            available_resolutions = []
            for uri_entry in matching_component.get('uris', []):
                for res_entry in uri_entry.get('resolutions', []):
                    resolution = res_entry.get('resolution', '')
                    formats = res_entry.get('formats', [])
                    if any(f.get('mimeType') == mime_type for f in formats):
                        available_resolutions.append(resolution)
            if available_resolutions:
                return _pick_best_resolution(available_resolutions, preferred_size)

    if 'maps' in asset_metadata:
        available_resolutions = []
        for map_entry in asset_metadata['maps']:
            if (map_entry.get('type') == component_type and
                    map_entry.get('mimeType') == mime_type):
                resolution = map_entry.get('resolution', '')
                if resolution:
                    available_resolutions.append(resolution)
        if available_resolutions:
            return _pick_best_resolution(available_resolutions, preferred_size)

    return None


def _pick_best_resolution(available_resolutions, preferred_size):
    def get_max_dimension(res_str):
        try:
            parts = res_str.split('x')
            return max(int(parts[0]), int(parts[1]))
        except:
            return 0

    preferred_size = int(preferred_size)
    for res in available_resolutions:
        if get_max_dimension(res) == preferred_size:
            return res
    available_resolutions.sort(key=get_max_dimension, reverse=True)
    return available_resolutions[0]


def get_types_present_in_metadata(asset_metadata):
    """
    Return the set of component type strings actually present in this asset's metadata.
    Returns None if metadata is unavailable (meaning: don't filter, try everything).
    """
    if not asset_metadata:
        return None

    present = set()

    if 'components' in asset_metadata:
        for component in asset_metadata['components']:
            t = component.get('type', '').lower()
            if t:
                present.add(t)
    elif 'maps' in asset_metadata:
        for map_entry in asset_metadata['maps']:
            t = map_entry.get('type', '').lower()
            if t:
                present.add(t)

    return present if present else None


def generate_resolution_fallback_list():
    sizes = [8192, 4096, 2048, 1024]
    resolutions = []
    for size in sizes:
        resolutions.append(f"{size}x{size}")
        for other_size in sizes:
            if size != other_size:
                resolutions.append(f"{size}x{other_size}")
    seen = set()
    unique_resolutions = []
    for res in resolutions:
        if res not in seen:
            seen.add(res)
            unique_resolutions.append(res)
    return unique_resolutions


def downloadAsset(id):
    global successful_downloads, failed_downloads

    fallback_resolutions = generate_resolution_fallback_list()

    try:
        heads = {
            "authorization": "Bearer " + authToken,
            "content-type": "application/json;charset=UTF-8",
            "Accept": "application/json"
        }

        all_component_types = [
            {"type": t["type"], "mimeType": t["mimeType"], "preferred_size": t["resolution"]}
            for t in texture_settings["textures"]
            if t.get("enabled", True)
        ]

        print(f"  --> Fetching metadata for {id}...")
        asset_metadata = get_asset_metadata(id)

        all_component_types = check_and_add_missing_types(asset_metadata, id, all_component_types)

        # ---------------------------------------------------------------
        # Detect asset type and build the components list accordingly
        # ---------------------------------------------------------------
        asset_is_3d = is_3d_asset(asset_metadata)

        if asset_is_3d:
            # 3D assets: one component entry per (type × lod_index)
            # Use the preferred_size from whichever comp_def matches, default 4096
            def get_preferred_size(comp_type):
                for c in all_component_types:
                    if c["type"] == comp_type:
                        return c["preferred_size"]
                return 4096

            # Pick the preferred mimeType per component type from our list
            def get_mime_type(comp_type):
                for c in all_component_types:
                    if c["type"] == comp_type:
                        return c["mimeType"]
                return "image/jpeg"

            lod_indices = get_lod_count_from_metadata(asset_metadata)
            print(f"  --> 3D asset detected, LOD levels: {lod_indices}")

            # We'll manage active_components as a set of (type, lod_idx) to allow
            # per-LOD exclusion during fallback
            active_lod_components = []
            for comp in asset_metadata.get('components', []):
                comp_type = comp.get('type', '').lower()
                if not comp_type:
                    continue
                mime = get_mime_type(comp_type)
                preferred = get_preferred_size(comp_type)
                # Detect once whether this component has any LOD-tagged formats
                has_lod_formats = any(
                    fmt.get('lodType') == 'lod'
                    for uri_entry in comp.get('uris', [])
                    for res_entry in uri_entry.get('resolutions', [])
                    for fmt in res_entry.get('formats', [])
                )
                for lod_idx in lod_indices:
                    res, actual_mime = _find_resolution_for_3d_lod(comp, mime, preferred, lod_idx)
                    if res:
                        active_lod_components.append({
                            "type": comp_type,
                            "mimeType": actual_mime,
                            "preferred_size": preferred,
                            "lod": lod_idx,
                            "resolution": res,
                            "is_flat": not has_lod_formats  # flat = no lod key in payload
                        })

            skipped_types = [c["type"] for c in all_component_types
                             if c["type"] not in {x["type"] for x in active_lod_components}]
            if skipped_types:
                print(f"  --> Skipping types not in 3D asset: {', '.join(set(skipped_types))}")

            # Guard: if nothing resolved, fall back to server defaults immediately
            if not active_lod_components:
                print(f"  --> WARNING: No resolvable components found for {id}, using server defaults...")
                payload_no_components = {
                    "asset": id,
                    "config": {
                        "meshMimeType": "application/x-fbx",
                        "lowerlod_meshes": True,
                        "lowerlod_normals": True,
                        "maxlod": 0,
                        "highpoly": False,
                    }
                }
                try:
                    sdr = requests.post("https://quixel.com/v1/downloads", headers=heads,
                                        data=json.dumps(payload_no_components), timeout=10)
                except requests.Timeout:
                    print(f"  --> ERROR: Server defaults request timed out for {id}")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id, "No resolvable components: server defaults timed out")
                    return False
                if sdr.ok:
                    download_request_response_json = sdr.json()
                    print(f"  --> SUCCESS (using server defaults — no texture components resolved)")
                    success = True
                else:
                    se = sdr.json()
                    print(f"  --> ERROR: Server defaults failed for {id} | [{se.get('code')}] {se.get('msg')}")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id, f"No resolvable components + server defaults failed: [{se.get('code')}] {se.get('msg')}")
                    return False

                # Skip the main download loop — go straight to file download
                # by jumping to the file-fetch section below via a flag
                # (restructure: just do the file fetch inline here)
                try:
                    download_response = requests.get(
                        f"https://assetdownloads.quixel.com/download/{download_request_response_json['id']}?preserveStructure=true&url=https%3A%2F%2Fquixel.com%2Fv1%2Fdownloads",
                        timeout=60)
                except requests.Timeout:
                    print(f"  --> ERROR: File download timed out for {id}, skipping...")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id)
                    return False
                if not download_response.ok:
                    err = download_response.json()
                    print(f"  --> ERROR: Unable to download {id} | [{err['code']}] {err['msg']}")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id)
                    return False
                filename = re.findall("filename=(.+)", download_response.headers['content-disposition'])[0]
                with open(filename, mode="wb") as file:
                    file.write(download_response.content)
                if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                    print(f"  --> ERROR: File {filename} was not written successfully")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id)
                    return False
                with cache_lock:
                    cache.append(id)
                save_cache()
                remove_from_failed(id)
                with counter_lock:
                    successful_downloads += 1
                    print(f"  --> DOWNLOADED {id} | {filename} | {successful_downloads} / {num_to_download}")
                return True

            # Config for 3D assets — try combinations to find what produces LOD meshes
            mesh_lod_count = len([m for m in asset_metadata.get('meshes', [])
                                   if m.get('type') == 'lod'])
            max_lod_value = max(lod_indices) if lod_indices else max(mesh_lod_count - 1, 0)

            # Try each config variant; report what the API returns for each
            # Derive maxlod from mesh LOD count
            max_lod_value = max(lod_indices) if lod_indices else max(mesh_lod_count - 1, 0)

            import zipfile, io as _io

            # Build lods array: all LOD tris from metadata, filtered to enabled_lods indices
            all_lod_meshes = [m for m in asset_metadata.get('meshes', [])
                              if m.get('type') == 'lod']
            # enabled_lods is a list of indices (0-based); clamp to what this asset has
            max_available_lod = len(all_lod_meshes) - 1
            active_lod_indices = [i for i in enabled_lods if i <= max_available_lod]
            lod_tris = [all_lod_meshes[i].get('tris', 0) for i in active_lod_indices]

            if not lod_tris:
                print(f"  --> WARNING: No LOD meshes available for requested LODs {enabled_lods}, "
                      f"asset only has {len(all_lod_meshes)} LODs")
                lod_tris = [m.get('tris', 0) for m in all_lod_meshes]
                active_lod_indices = list(range(len(all_lod_meshes)))

            print(f"  --> Mesh LODs: indices={active_lod_indices}, tris={lod_tris}")

            # Flat texture components — deduplicated, one entry per type
            request_components = []
            seen_types = set()
            for c in active_lod_components:
                t = c.get('type', '')
                if t not in seen_types:
                    seen_types.add(t)
                    request_components.append({
                        'type': t,
                        'mimeType': c.get('mimeType', 'image/jpeg'),
                        'resolution': c.get('resolution', '4096x4096'),
                    })

            base_config = {
                "meshMimeType": mesh_mime_type,
                "highpoly": False,
                "ztool": False,
                "lowerlod_normals": True,
                "albedo_lods": albedo_lods_flag,
                "lods": lod_tris,
                "brushes": False,
            }

            def probe_zip(label, payload):
                try:
                    tr = requests.post("https://quixel.com/v1/downloads", headers=heads,
                                       data=json.dumps(payload), timeout=10)
                except requests.Timeout:
                    print(f"    {label}: TIMEOUT on POST"); return
                if not tr.ok:
                    print(f"    {label}: POST FAIL {tr.status_code} {tr.text[:80]}"); return
                dl_id = tr.json().get("id", "")
                dl_url = f"https://assetdownloads.quixel.com/download/{dl_id}?preserveStructure=true&url=https%3A%2F%2Fquixel.com%2Fv1%2Fdownloads"
                try:
                    zr = requests.get(dl_url, timeout=60)
                except requests.Timeout:
                    print(f"    {label}: TIMEOUT fetching zip"); return
                if not zr.ok:
                    print(f"    {label}: ZIP FAIL {zr.status_code}"); return
                try:
                    zf = zipfile.ZipFile(_io.BytesIO(zr.content))
                    names = zf.namelist()
                    meshes = sorted([n for n in names if n.endswith(('.fbx','.abc','.obj'))])
                    normals = [n for n in names if 'Normal' in n and 'LOD' in n]
                    print(f"    {label}: {len(names)} files | meshes={meshes} | lod_normals={len(normals)}")
                except Exception as e:
                    print(f"    {label}: ZIP PARSE ERROR {e}")

            # For 3D assets: POST to /v1/downloads to get pre-signed CDN URLs,
            # then download each file directly. The response contains:
            #   assetDownload.components[] - flat textures with CDN URLs
            #   assetDownload.meshes[]     - mesh files with CDN URLs (only highpoly by default)
            # We extract the signing params and build URLs for all LOD meshes + textures.

            # POST to /v1/downloads to get a download record ID,
            # then fetch the zip from assetdownloads.quixel.com with preserveStructure=true
            # which includes all LOD meshes and textures.
            payload_3d = {
                "asset": id,
                "config": base_config,
                "components": request_components,
            }
            print(f"  --> 3D payload: lods={lod_tris}, components={len(request_components)}")
            try:
                dl_post = requests.post("https://quixel.com/v1/downloads", headers=heads,
                    data=json.dumps(payload_3d), timeout=10)
            except requests.Timeout:
                print(f"  --> ERROR: Download request timed out for {id}")
                with counter_lock:
                    failed_downloads += 1
                track_failed_download(id)
                return False
            if not dl_post.ok:
                err = dl_post.json()
                error_code = err.get('code', err.get('error', 'UNKNOWN'))
                error_msg = err.get('msg', err.get('message', 'Unknown error'))
                print(f"  --> ERROR: Download request failed for {id} | [{error_code}] {error_msg}")
                with counter_lock:
                    failed_downloads += 1
                track_failed_download(id, f"[{error_code}] {error_msg}")
                return False

            dl_id = dl_post.json().get('id', '')
            dl_url = f"https://assetdownloads.quixel.com/download/{dl_id}?preserveStructure=true&url=https%3A%2F%2Fquixel.com%2Fv1%2Fdownloads"

            try:
                download_response = requests.get(dl_url, timeout=120)
            except requests.Timeout:
                print(f"  --> ERROR: File download timed out for {id}")
                with counter_lock:
                    failed_downloads += 1
                track_failed_download(id)
                return False
            if not download_response.ok:
                err = download_response.json()
                print(f"  --> ERROR: Unable to download {id} | [{err.get('code','?')}] {err.get('msg','?')}")
                with counter_lock:
                    failed_downloads += 1
                track_failed_download(id)
                return False

            filename = re.findall(r"filename=(.+)", download_response.headers.get('content-disposition',''))[0]
            with open(filename, mode="wb") as f_out:
                f_out.write(download_response.content)

            with cache_lock:
                cache.append(id)
            save_cache()
            remove_from_failed(id)
            with counter_lock:
                successful_downloads += 1
                print(f"  --> DOWNLOADED {id} | {filename} | {successful_downloads} / {num_to_download}")
            return True


        else:
            # Flat-map assets (surfaces, 3D plants): original logic
            present_types = get_types_present_in_metadata(asset_metadata)
            if present_types is not None:
                active_flat_components = [c for c in all_component_types if c["type"] in present_types]
                skipped = [c["type"] for c in all_component_types if c["type"] not in present_types]
                if skipped:
                    print(f"  --> Skipping {len(skipped)} component types not in asset: {', '.join(skipped)}")
            else:
                active_flat_components = all_component_types.copy()

            asset_config = {
                "meshMimeType": "application/x-fbx",
                "lowerlod_meshes": True,
                "lowerlod_normals": True,
                "maxlod": 0,
                "highpoly": True,
            }

        # ---------------------------------------------------------------
        # Download request loop
        # ---------------------------------------------------------------
        download_request_response_json = None
        success = False
        excluded_types = []  # track excluded types for logging

        while True:
            # Build the components array for the payload
            if asset_is_3d:
                components = []
                for c in active_lod_components:
                    entry = {
                        "type": c["type"],
                        "resolution": c["resolution"],
                        "mimeType": c["mimeType"],
                    }
                    if not c.get("is_flat", False):
                        entry["lod"] = c["lod"]
                    components.append(entry)
            else:
                # Original flat-map logic
                active_components = active_flat_components  # alias for error handling below
                components = []
                for comp in active_flat_components:
                    resolution = find_resolution_for_component(
                        asset_metadata, comp["type"], comp["mimeType"], comp["preferred_size"]
                    )
                    if not resolution:
                        resolution = f"{comp['preferred_size']}x{comp['preferred_size']}"
                    components.append({
                        "type": comp["type"],
                        "resolution": resolution,
                        "mimeType": comp["mimeType"]
                    })

            payload = {
                "asset": id,
                "config": asset_config,
                "components": components
            }

            print(f"  --> DOWNLOADING ITEM {id}" + (" [3D+LODs]" if asset_is_3d else ""))
            print(f"  --> DEBUG payload components ({len(components)} entries): "
                  + str([f"{c['type']}{'@LOD'+str(c['lod']) if 'lod' in c else '[flat]'} {c['resolution']} {c['mimeType']}" for c in components[:8]])
                  + ("..." if len(components) > 8 else ""))

            # For 3D assets: use mesh-only payload (no components array).
            # The config drives what the API bundles:
            #   highpoly:False + lowerlod_meshes:True -> LOD meshes (_LOD0-N.fbx)
            #   lowerlod_normals:True -> baked normal maps per LOD
            #   maxlod:N -> how many LOD levels to include
            if asset_is_3d:
                mesh_only_payload = {"asset": id, "config": asset_config}
                try:
                    mo_response = requests.post(
                        "https://quixel.com/v1/downloads", headers=heads,
                        data=json.dumps(mesh_only_payload), timeout=10
                    )
                except requests.Timeout:
                    mo_response = None
                if mo_response is not None and mo_response.ok:
                    download_request_response_json = mo_response.json()
                    success = True
                    break
                elif mo_response is not None:
                    raw_body = mo_response.text
                    http_status = mo_response.status_code
                    try:
                        err = mo_response.json()
                        error_code = err.get('code', err.get('error', 'UNKNOWN'))
                        error_msg = err.get('msg', err.get('message', 'Unknown error'))
                    except Exception:
                        error_code = f"HTTP_{http_status}"
                        error_msg = raw_body[:500] if raw_body else "Empty response"
                    print(f"  --> ERROR: Unable to download {id} | [{error_code}] {error_msg}")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id, f"[{error_code}] {error_msg}")
                    return False
                else:
                    print(f"  --> ERROR: Download request timed out for {id}")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id)
                    return False

            try:
                download_request_response = requests.post(
                    "https://quixel.com/v1/downloads", headers=heads,
                    data=json.dumps(payload), timeout=10
                )
            except requests.Timeout:
                print(f"  --> ERROR: Download request timed out for {id}, skipping...")
                with counter_lock:
                    failed_downloads += 1
                track_failed_download(id)
                return False

            if download_request_response.ok:
                download_request_response_json = download_request_response.json()
                if excluded_types:
                    print(f"  --> SUCCESS (excluded types: {', '.join(excluded_types)})")
                else:
                    print(f"  --> SUCCESS")
                success = True
                break

            else:
                raw_body = download_request_response.text
                http_status = download_request_response.status_code
                try:
                    download_request_response_err = download_request_response.json()
                    error_code = download_request_response_err.get('code', 'UNKNOWN')
                    error_msg = download_request_response_err.get('msg', download_request_response_err.get('message', 'Unknown error'))
                except Exception:
                    error_code = f"HTTP_{http_status}"
                    error_msg = raw_body[:500] if raw_body else "Empty response"
                print(f"  --> DEBUG: HTTP {http_status} | raw: {raw_body[:300]}")

                if error_code == "ACCESS_DENIED":
                    print(f"  --> WARNING: ACCESS_DENIED, trying resolution stepping...")
                    found_working_resolution = False
                    for fallback_res in fallback_resolutions:
                        if asset_is_3d:
                            test_components = []
                            for c in active_lod_components:
                                entry = {"type": c["type"], "resolution": fallback_res, "mimeType": c["mimeType"]}
                                if not c.get("is_flat", False):
                                    entry["lod"] = c["lod"]
                                test_components.append(entry)
                        else:
                            test_components = [
                                {"type": c["type"], "resolution": fallback_res, "mimeType": c["mimeType"]}
                                for c in active_flat_components
                            ]
                        test_payload = {"asset": id, "config": asset_config, "components": test_components}
                        try:
                            test_response = requests.post(
                                "https://quixel.com/v1/downloads", headers=heads,
                                data=json.dumps(test_payload), timeout=10
                            )
                        except requests.Timeout:
                            continue
                        if test_response.ok:
                            print(f"  --> SUCCESS: Found accessible resolution {fallback_res}")
                            download_request_response_json = test_response.json()
                            found_working_resolution = True
                            success = True
                            break

                    if not found_working_resolution:
                        print(f"  --> WARNING: Resolution stepping failed, trying server defaults...")
                        payload_no_components = {
                            "asset": id,
                            "config": {
                                "meshMimeType": "application/x-fbx",
                                "lowerlod_meshes": True,
                                "lowerlod_normals": True,
                                "maxlod": asset_config.get("maxlod", 0),
                                "highpoly": False,
                            }
                        }
                        try:
                            server_default_response = requests.post(
                                "https://quixel.com/v1/downloads", headers=heads,
                                data=json.dumps(payload_no_components), timeout=10
                            )
                        except requests.Timeout:
                            print(f"  --> ERROR: Server defaults request timed out for {id}")
                            with counter_lock:
                                failed_downloads += 1
                            track_failed_download(id)
                            return False
                        if server_default_response.ok:
                            download_request_response_json = server_default_response.json()
                            print(f"  --> SUCCESS (using server defaults)")
                            success = True
                        else:
                            server_error = server_default_response.json()
                            print(f"  --> ERROR: Server defaults also failed | [{server_error.get('code', 'UNKNOWN')}] {server_error.get('msg', 'Unknown error')}")
                            with counter_lock:
                                failed_downloads += 1
                            track_failed_download(id)
                            return False
                    break

                elif error_code == "INVALID_PAYLOAD" and "resolution not found" in error_msg:
                    try:
                        json_match = re.search(r'\{[^}]+\}', error_msg)
                        if json_match:
                            failing_component = json.loads(json_match.group())
                            failing_type = failing_component.get('type')
                            failing_mime = failing_component.get('mimeType')
                            failing_lod = failing_component.get('lod')  # may be None for flat assets

                            print(f"  --> WARNING: Resolution not found for '{failing_type}'"
                                  + (f" LOD{failing_lod}" if failing_lod is not None else "")
                                  + ", trying fallback resolutions...")

                            found_working_resolution = False
                            for fallback_res in fallback_resolutions:
                                if asset_is_3d:
                                    test_components = []
                                    for c in active_lod_components:
                                        use_res = fallback_res if (c["type"] == failing_type and c["lod"] == failing_lod) else c["resolution"]
                                        entry = {"type": c["type"], "resolution": use_res, "mimeType": c["mimeType"]}
                                        if not c.get("is_flat", False):
                                            entry["lod"] = c["lod"]
                                        test_components.append(entry)
                                else:
                                    test_components = []
                                    for comp in active_flat_components:
                                        if comp["type"] == failing_type:
                                            test_components.append({
                                                "type": comp["type"], "resolution": fallback_res,
                                                "mimeType": comp["mimeType"]
                                            })
                                        else:
                                            orig_res = find_resolution_for_component(
                                                asset_metadata, comp["type"], comp["mimeType"], comp["preferred_size"]
                                            ) or f"{comp['preferred_size']}x{comp['preferred_size']}"
                                            test_components.append({
                                                "type": comp["type"], "resolution": orig_res,
                                                "mimeType": comp["mimeType"]
                                            })

                                test_payload = {"asset": id, "config": asset_config, "components": test_components}
                                try:
                                    test_response = requests.post(
                                        "https://quixel.com/v1/downloads", headers=heads,
                                        data=json.dumps(test_payload), timeout=10
                                    )
                                except requests.Timeout:
                                    continue
                                if test_response.ok:
                                    print(f"  --> SUCCESS: Found working resolution {fallback_res} for '{failing_type}'")
                                    download_request_response_json = test_response.json()
                                    found_working_resolution = True
                                    success = True
                                    break
                                else:
                                    err = test_response.json()
                                    if err.get('code') == 'INVALID_PAYLOAD':
                                        break
                                    else:
                                        print(f"  --> ERROR: Unexpected error during fallback: [{err.get('code')}] {err.get('msg', '')}")
                                        with counter_lock:
                                            failed_downloads += 1
                                        track_failed_download(id, f"Unexpected error during fallback: [{err.get('code')}] {err.get('msg')}")
                                        return False

                            if not found_working_resolution:
                                # Exclude the failing (type, lod) combo and retry
                                if asset_is_3d:
                                    before = len(active_lod_components)
                                    active_lod_components = [
                                        c for c in active_lod_components
                                        if not (c["type"] == failing_type and c["lod"] == failing_lod)
                                    ]
                                    print(f"  --> WARNING: Excluding '{failing_type}' LOD{failing_lod} "
                                          f"({before - len(active_lod_components)} entries removed)")
                                    excluded_types.append(f"{failing_type}_LOD{failing_lod}")
                                    if not active_lod_components:
                                        print(f"  --> WARNING: No valid components remaining, trying server defaults...")
                                        payload_no_components = {"asset": id, "config": {
                                            "meshMimeType": "application/x-fbx",
                                            "lowerlod_meshes": True, "lowerlod_normals": True,
                                            "maxlod": asset_config.get("maxlod", 0), "highpoly": False,
                                        }}
                                        try:
                                            sdr = requests.post("https://quixel.com/v1/downloads", headers=heads,
                                                                data=json.dumps(payload_no_components), timeout=10)
                                        except requests.Timeout:
                                            with counter_lock: failed_downloads += 1
                                            track_failed_download(id, "No valid components: Server defaults timed out")
                                            return False
                                        if sdr.ok:
                                            download_request_response_json = sdr.json()
                                            print(f"  --> SUCCESS (using server defaults)")
                                            success = True
                                            break
                                        else:
                                            se = sdr.json()
                                            print(f"  --> ERROR: No valid component types remaining for {id}")
                                            with counter_lock: failed_downloads += 1
                                            track_failed_download(id, f"No valid components: [{se.get('code')}] {se.get('msg')}")
                                            return False
                                    continue  # retry loop
                                else:
                                    active_flat_components = [c for c in active_flat_components if c['type'] != failing_type]
                                    excluded_types.append(failing_type)
                                    if not active_flat_components:
                                        print(f"  --> WARNING: No valid components remaining, trying server defaults...")
                                        payload_no_components = {"asset": id, "config": {
                                            "meshMimeType": "application/x-fbx",
                                            "lowerlod_meshes": True, "lowerlod_normals": True,
                                            "maxlod": 0, "highpoly": False,
                                        }}
                                        try:
                                            sdr = requests.post("https://quixel.com/v1/downloads", headers=heads,
                                                                data=json.dumps(payload_no_components), timeout=10)
                                        except requests.Timeout:
                                            with counter_lock: failed_downloads += 1
                                            track_failed_download(id, "No valid components: Server defaults timed out")
                                            return False
                                        if sdr.ok:
                                            download_request_response_json = sdr.json()
                                            print(f"  --> SUCCESS (using server defaults)")
                                            success = True
                                            break
                                        else:
                                            se = sdr.json()
                                            print(f"  --> ERROR: No valid component types remaining for {id}")
                                            with counter_lock: failed_downloads += 1
                                            track_failed_download(id, f"No valid components: [{se.get('code')}] {se.get('msg')}")
                                            return False
                                    continue
                            else:
                                # Update the resolved resolution in active_lod_components for future retries
                                if asset_is_3d:
                                    for c in active_lod_components:
                                        if c["type"] == failing_type and c["lod"] == failing_lod:
                                            c["resolution"] = fallback_res
                                            # mimeType was already correct since test_components used c["mimeType"]
                                break
                        else:
                            print(f"  --> ERROR: Unable to parse error message | [{error_code}] {error_msg}")
                            with counter_lock:
                                failed_downloads += 1
                            track_failed_download(id, f"Could not parse INVALID_PAYLOAD error: {error_msg}")
                            return False
                    except Exception as e:
                        print(f"  --> ERROR: Exception while handling resolution error | {e}")
                        with counter_lock:
                            failed_downloads += 1
                        track_failed_download(id, f"Exception handling resolution error: {e}")
                        return False

                elif error_code == "INVALID_PAYLOAD" and "type not found" in error_msg:
                    try:
                        json_match = re.search(r'\{[^}]+\}', error_msg)
                        if json_match:
                            failing_component = json.loads(json_match.group())
                            failing_type = failing_component.get('type')
                            failing_lod = failing_component.get('lod')

                            print(f"  --> WARNING: Type '{failing_type}'"
                                  + (f" LOD{failing_lod}" if failing_lod is not None else "")
                                  + " not available, trying fallback resolutions...")

                            found_working_resolution = False
                            for fallback_res in fallback_resolutions:
                                if asset_is_3d:
                                    test_components = []
                                    for c in active_lod_components:
                                        use_res = fallback_res if (c["type"] == failing_type and c["lod"] == failing_lod) else c["resolution"]
                                        entry = {"type": c["type"], "resolution": use_res, "mimeType": c["mimeType"]}
                                        if not c.get("is_flat", False):
                                            entry["lod"] = c["lod"]
                                        test_components.append(entry)
                                else:
                                    test_components = []
                                    for comp in active_flat_components:
                                        if comp["type"] == failing_type:
                                            test_components.append({
                                                "type": comp["type"], "resolution": fallback_res,
                                                "mimeType": comp["mimeType"]
                                            })
                                        else:
                                            orig_res = find_resolution_for_component(
                                                asset_metadata, comp["type"], comp["mimeType"], comp["preferred_size"]
                                            ) or f"{comp['preferred_size']}x{comp['preferred_size']}"
                                            test_components.append({
                                                "type": comp["type"], "resolution": orig_res,
                                                "mimeType": comp["mimeType"]
                                            })

                                test_payload = {"asset": id, "config": asset_config, "components": test_components}
                                try:
                                    test_response = requests.post(
                                        "https://quixel.com/v1/downloads", headers=heads,
                                        data=json.dumps(test_payload), timeout=10
                                    )
                                except requests.Timeout:
                                    continue
                                if test_response.ok:
                                    print(f"  --> SUCCESS: Found working resolution {fallback_res} for '{failing_type}'")
                                    download_request_response_json = test_response.json()
                                    found_working_resolution = True
                                    success = True
                                    break

                            if not found_working_resolution:
                                if asset_is_3d:
                                    active_lod_components = [
                                        c for c in active_lod_components
                                        if not (c["type"] == failing_type and c["lod"] == failing_lod)
                                    ]
                                    excluded_types.append(f"{failing_type}_LOD{failing_lod}" if failing_lod is not None else failing_type)
                                    if not active_lod_components:
                                        print(f"  --> ERROR: No valid component types remaining for {id}")
                                        with counter_lock: failed_downloads += 1
                                        track_failed_download(id)
                                        return False
                                    continue
                                else:
                                    active_flat_components = [c for c in active_flat_components if c['type'] != failing_type]
                                    excluded_types.append(failing_type)
                                    if not active_flat_components:
                                        print(f"  --> ERROR: No valid component types remaining for {id}")
                                        with counter_lock: failed_downloads += 1
                                        track_failed_download(id)
                                        return False
                                    continue
                            else:
                                if asset_is_3d:
                                    for c in active_lod_components:
                                        if c["type"] == failing_type and c["lod"] == failing_lod:
                                            c["resolution"] = fallback_res
                                break
                        else:
                            print(f"  --> ERROR: Unable to parse error message | [{error_code}] {error_msg}")
                            with counter_lock:
                                failed_downloads += 1
                            track_failed_download(id)
                            return False
                    except Exception as e:
                        print(f"  --> ERROR: Exception while handling error | {e}")
                        with counter_lock:
                            failed_downloads += 1
                        track_failed_download(id)
                        return False

                else:
                    print(f"  --> ERROR: Unable to download {id} | [{error_code}] {error_msg}")
                    with counter_lock:
                        failed_downloads += 1
                    track_failed_download(id)
                    return False

            if success:
                break

        if not success or download_request_response_json is None:
            with counter_lock:
                failed_downloads += 1
            track_failed_download(id)
            return False

        # Download the actual file
        try:
            download_response = requests.get(
                f"https://assetdownloads.quixel.com/download/{download_request_response_json['id']}?url=https%3A%2F%2Fquixel.com%2Fv1%2Fdownloads",
                timeout=60)
        except requests.Timeout:
            print(f"  --> ERROR: File download timed out for {id}, skipping...")
            with counter_lock:
                failed_downloads += 1
            track_failed_download(id)
            return False

        if not download_response.ok:
            download_response = download_response.json()
            print(f"  --> ERROR: Unable to download {id} | [{download_response['code']}] {download_response['msg']}")
            with counter_lock:
                failed_downloads += 1
            track_failed_download(id)
            return False

        filename = re.findall("filename=(.+)", download_response.headers['content-disposition'])[0]

        with open(filename, mode="wb") as file:
            file.write(download_response.content)

        if not os.path.exists(filename) or os.path.getsize(filename) == 0:
            print(f"  --> ERROR: File {filename} was not written successfully")
            with counter_lock:
                failed_downloads += 1
            track_failed_download(id)
            return False

        with cache_lock:
            cache.append(id)
        save_cache()
        remove_from_failed(id)

        with counter_lock:
            successful_downloads += 1
            print(f"  --> DOWNLOADED {id} | {filename} | {successful_downloads} / {num_to_download}")

        return True

    except Exception as e:
        print(f"  --> ERROR: Download of {id} failed due to:", e)
        with counter_lock:
            failed_downloads += 1
        track_failed_download(id)
        return False


# 2. Ask user for asset type filter
print("=" * 60)
print("Megascans Asset Downloader v6")
print("(3D asset LOD support + Resolution stepping + Server defaults fallback)")
print("=" * 60)

available_types = list_available_asset_types()
failed_ids = load_failed_ids()
has_failed = len(failed_ids) > 0

if available_types:
    print("\n-> Available asset types:")
    for i, asset_type in enumerate(sorted(available_types), 1):
        print(f"   {i}. {asset_type}")
    print(f"   {len(available_types) + 1}. ALL (no filter)")
    if has_failed:
        print(f"   {len(available_types) + 2}. download_failed ({len(failed_ids)} assets)")
else:
    print("\n-> No categorized asset files found in './asset_types/'")
    print("-> Will fetch all assets from API")
    if has_failed:
        print(f"   1. download_failed ({len(failed_ids)} assets)")

asset_type_choice = input("\n-> Enter asset type name or number (ENTER for ALL): ").strip()

selected_asset_type = None
retry_failed = False
if asset_type_choice:
    if asset_type_choice.isnumeric():
        choice_num = int(asset_type_choice)
        if 1 <= choice_num <= len(available_types):
            selected_asset_type = sorted(available_types)[choice_num - 1]
        elif choice_num == len(available_types) + 1:
            selected_asset_type = None
        elif choice_num == len(available_types) + 2 and has_failed:
            retry_failed = True
        else:
            print("-> Invalid choice, using ALL assets")
    else:
        if asset_type_choice == "download_failed" and has_failed:
            retry_failed = True
        elif asset_type_choice in available_types:
            selected_asset_type = asset_type_choice
        else:
            print(f"-> '{asset_type_choice}' not found, using ALL assets")

if retry_failed:
    print(f"\n-> Selected: download_failed (retrying {len(failed_ids)} failed assets)")
elif selected_asset_type:
    print(f"\n-> Selected asset type: {selected_asset_type}")
else:
    print(f"\n-> Selected: ALL assets (no filter)")

print("\n-> Get Acquired Items and filter for cache...")
if retry_failed:
    items = [x for x in failed_ids if x not in cache]
else:
    raw_items = getAcquiredIds(selected_asset_type)
    if selected_asset_type is not None and len(failed_ids) > 0:
        items = [x for x in raw_items if x not in cache and x not in failed_ids]
        skipped_count = len([x for x in raw_items if x in failed_ids and x not in cache])
        if skipped_count > 0:
            print(f"-> Skipping {skipped_count} previously failed assets (select 'download_failed' to retry them)")
    else:
        items = [x for x in raw_items if x not in cache]

if len(items) == 0:
    print("-> No new items to download (all items are already in cache)")
    sys.exit(0)

# 3. Get number to download
correct = False
num_to_download = len(items)
while not correct:
    num_to_download_input = input(
        f"\n-> How many of your {len(items)} assets do you want to download? (ALL to download everything) >> ")
    if num_to_download_input == "ALL":
        correct = True
    elif num_to_download_input.isnumeric():
        num_to_download_int = int(num_to_download_input)
        if 0 < num_to_download_int <= len(items):
            num_to_download = num_to_download_int
            correct = True
        else:
            print("-> ERROR: Input needs to be above 0 and below the total number of items you can download.")
    else:
        print("-> ERROR: Need to enter ALL or an integer.")

# 4. Get max number of workers and download items
correct = False
num_of_threads = 4
while not correct:
    num_of_threads_input = input(f"-> How many workers do you want? (ENTER to use 4) >> ")
    if num_of_threads_input == "":
        correct = True
    elif num_of_threads_input.isnumeric():
        num_of_threads_input_int = int(num_of_threads_input)
        if num_of_threads_input_int > 0:
            num_of_threads = num_of_threads_input_int
            correct = True
        else:
            print("-> ERROR: Input needs to be above 0 workers.")
    else:
        print("-> ERROR: Need to enter nothing or an integer.")

print(f"\n-> Downloading {num_to_download} assets...")
items_to_process = items[:num_to_download]

with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
    executor.map(downloadAsset, items_to_process)

print(f"\n-> Successfully downloaded {successful_downloads} assets")
if failed_downloads > 0:
    print(f"-> {failed_downloads} downloads failed")

print("-> Saving final cache...")
save_cache()

if missing_types_data["discovered_types"]:
    print("\n" + "=" * 60)
    print("NEW COMPONENT TYPES DISCOVERED:")
    print("=" * 60)
    for type_entry in missing_types_data["discovered_types"]:
        count = sum(1 for assets in missing_types_data["assets_with_missing_types"].values()
                    if type_entry["type"] in assets)
        print(f"  - {type_entry['type']} (mimeType: {type_entry['mimeType']}, found in {count} assets)")
    print(f"\nDetails saved to: {MISSING_TYPES_FILE}")

print("\n" + "=" * 60)
print("DONE! Congrats on your new assets.")
print("=" * 60)
if failed_downloads > 0:
    print(f"\nNote: {failed_downloads} items failed to download. You may want to run the script again to retry failed items.")