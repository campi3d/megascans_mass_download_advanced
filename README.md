# Megascans Mass Downloader

A Python script to bulk-download your purchased Megascans assets.
This is a more advanced version of what can be found here: https://gist.github.com/aldenparker/0d8fee85469d3561bc3a772a03d642cb

Additional features are
- Download individual categories (3D Asset, Surface, Decal etc.)
- Specify resolution for textures
- Specify LOD levels for 3D assets
- Retry failed downloads
- Skip downloading certain texture maps

---

## Setup & Usage

### 1. Download & Place Files
Download all files and place them in a folder, preserving the structure from this repository.
The `megascans_downloader.py` file should sit in the folder where your downloaded assets will be saved.

```
your_download_folder/
├── megascans_downloader.py
└── configuration/
    ├── authentication.txt
    ├── texture_settings.json
    └── mesh_lod_settings.json
```

---

### 2. Log In to Quixel
Go to [quixel.com/megascans/purchased](https://quixel.com/megascans/purchased) and log in with your account.

---

### 3. Open the Browser Console
Press **F12** to open DevTools and select the **Console** tab.

---

### 4. Get Your Auth Token
Paste the contents of `configuration/auth.js` into the console and press **Enter**.
Copy the output line that appears.

---

### 5. Save Your Token
Paste the copied token into `configuration/authentication.txt`, replacing the placeholder line that says `YOUR_TOKEN_HERE`.

> ⚠️ **Tokens expire after a few hours.** You will need to repeat steps 3–5 each time you start a new download session.

---

### 6. Run the Script
From your download folder, run:

```bash
python megascans_downloader.py
```

---

### 7. Follow the Prompts
The script will ask you a few questions at startup:

- **Asset type** — choose a specific category (e.g. `surface`, `3D asset`) or press Enter to download all
- **How many assets** — enter a number or `ALL`
- **Number of workers** — the number of simultaneous download threads (default: **4**)

> 💡 **On workers:** increasing the worker count can speed things up significantly, since the script makes multiple server requests per asset to resolve download info. A good rule of thumb is to match the number of CPU cores you have available.

---

### 8. Download Cache
After each successful download the asset ID is written to `configuration/cache.txt`.

> ⚠️ **Do not delete `cache.txt`.** It prevents the script from re-downloading assets you already have. If deleted, the script will start over from scratch.

---

### 9. Failed Downloads
If a download fails, it is automatically logged to `configuration/.asset_types/download_failed.json`.
You can retry failed assets by selecting `download_failed` as the asset type on the next run.

---

## Configuration

### Texture Settings — `configuration/texture_settings.json`
Controls which texture maps are downloaded, their format, and resolution.

```json
{ "type": "albedo", "mimeType": "image/jpeg", "resolution": 4096, "enabled": true }
```

| Field | Options | Description |
|---|---|---|
| `type` | `albedo`, `normal`, `roughness`, … | The texture map type |
| `mimeType` | `image/jpeg`, `image/x-exr` | File format (JPEG or EXR) |
| `resolution` | `8192`, `4096`, `2048`, `1024` | Download resolution in pixels |
| `enabled` | `true` / `false` | Set to `false` to skip this map entirely |

---

### Mesh LOD Settings — `configuration/mesh_lod_settings.json`
Controls which LOD levels are downloaded for 3D assets.

```json
{
  "lods": [0, 1, 2, 3, 4, 5],
  "albedo_lods": true,
  "mesh_mime_type": "application/x-fbx"
}
```

| Field | Options | Description |
|---|---|---|
| `lods` | Array of integers | Which LOD levels to include. LOD 0 = highest detail |
| `albedo_lods` | `true` / `false` | Include per-LOD albedo remaps (typically LOD 4+) |
| `mesh_mime_type` | `application/x-fbx`, `application/x-abc`, `application/x-obj` | Mesh file format |

> **Example:** setting `"lods": [0, 1, 2]` downloads only the three highest-detail LODs and automatically excludes the LOD 3/4/5 normal maps.