((async () => {

  const getCookie = (name) => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
  }

  console.log("-> Checking Auth API Token...")
  let authToken = ""
  try {
    const authCookie = getCookie("auth") ?? "{}"
    authToken = JSON.parse(decodeURIComponent(authCookie))?.token
    if (!authToken) {
      return console.error("-> Error: cannot find authentication token. Please login again.")
    }
  } catch (_) {
    return console.error("-> Error: cannot find authentication token. Please login again.")
  }

  console.log(`-> Auth Token: ${authToken}`)
})())