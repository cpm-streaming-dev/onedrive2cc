import os
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, redirect, request, session, jsonify, url_for, Response
from flasgger import Swagger
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ["FLASK_SECRET_KEY"]
# Trust proxy headers from Cloudflare Tunnel (x_for, x_proto, x_host)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

Swagger(app, template={
    "swagger": "2.0",
    "info": {
        "title": "OneDrive → Confluent Cloud API",
        "description": (
            "Flask microservice that bridges Microsoft OneDrive (via Graph API) "
            "to Confluent Cloud Kafka. "
            "**Login first** at `/auth/login` before calling any other endpoint."
        ),
        "version": "1.0.0",
    },
    "tags": [
        {"name": "Auth", "description": "Microsoft OAuth2 login/logout"},
        {"name": "User", "description": "Current user profile"},
        {"name": "Files", "description": "Browse OneDrive files"},
        {"name": "Subscriptions", "description": "Manage Graph change notification subscriptions"},
        {"name": "Webhook", "description": "Graph webhook endpoint (called by Microsoft)"},
    ],
    "securityDefinitions": {},
    "schemes": ["http", "https"],
})

# ── Azure config ──────────────────────────────────────────────────────────────
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
REDIRECT_URI = os.environ.get("REDIRECT_URI", "http://localhost:5000/auth/callback")
TENANT = os.environ.get("AZURE_TENANT", "common")
AUTH_BASE = f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES = "Files.Read Files.ReadWrite offline_access User.Read"

# ── Confluent Cloud config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY", "")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "onedrive-files")

# ── Token persistence (file-based for POC) ────────────────────────────────────
# Webhook calls arrive with no user session, so tokens are stored on disk.
TOKEN_FILE = "tokens.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Kafka producer (lazy init) ────────────────────────────────────────────────
_producer = None
_delta_lock = threading.Lock()


def get_producer():
    global _producer
    if _producer is None:
        if not KAFKA_BOOTSTRAP:
            raise RuntimeError("KAFKA_BOOTSTRAP not configured")
        from confluent_kafka import Producer
        _producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": KAFKA_API_KEY,
            "sasl.password": KAFKA_API_SECRET,
        })
    return _producer


def send_to_kafka(item_id: str, record: dict):
    producer = get_producer()
    payload = json.dumps(record).encode("utf-8")
    producer.produce(
        topic=KAFKA_TOPIC,
        key=item_id.encode("utf-8"),
        value=payload,
    )
    producer.poll(0)
    log.info("Produced event for item %s to topic %s", item_id, KAFKA_TOPIC)


# ── Token persistence helpers ─────────────────────────────────────────────────

def _load_tokens() -> dict:
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            return json.load(f)
    return {}


def _save_tokens(data: dict):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f)


def _store_token(access_token: str, refresh_token: str, expires_in: int = 3600):
    expiry = (datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)).isoformat()
    _save_tokens({"access_token": access_token, "refresh_token": refresh_token, "expiry": expiry})
    session["access_token"] = access_token
    session["refresh_token"] = refresh_token


def _get_valid_token() -> str | None:
    """Return a valid access token, refreshing if needed. Works outside session context."""
    # Try session first
    token = session.get("access_token")
    tokens = _load_tokens()

    if not token:
        token = tokens.get("access_token")

    if not token:
        return None

    # Check expiry from file
    expiry_str = tokens.get("expiry")
    if expiry_str:
        expiry = datetime.fromisoformat(expiry_str)
        if datetime.now(timezone.utc) >= expiry:
            token = _do_refresh(tokens.get("refresh_token"))
    return token


def _do_refresh(refresh_token: str | None) -> str | None:
    if not refresh_token:
        return None
    resp = requests.post(f"{AUTH_BASE}/token", data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "scope": SCOPES,
    })
    if resp.ok:
        data = resp.json()
        _store_token(data["access_token"], data.get("refresh_token", refresh_token), data.get("expires_in", 3600))
        return data["access_token"]
    log.error("Token refresh failed: %s", resp.text)
    return None


# ── Graph helper ──────────────────────────────────────────────────────────────

def graph_request(method: str, path: str, **kwargs):
    token = _get_valid_token()
    if not token:
        raise PermissionError("No valid token available — user must authenticate at /auth/login")
    headers = {"Authorization": f"Bearer {token}"}
    url = path if path.startswith("https://") else f"{GRAPH_BASE}{path}"
    return requests.request(method, url, headers=headers, **kwargs)


def graph_get(path: str, **kwargs):
    return graph_request("GET", path, **kwargs)


def graph_post(path: str, **kwargs):
    return graph_request("POST", path, **kwargs)


def graph_delete(path: str, **kwargs):
    return graph_request("DELETE", path, **kwargs)


def graph_patch(path: str, **kwargs):
    return graph_request("PATCH", path, **kwargs)


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.get("/auth/login")
def login():
    """
    Redirect to Microsoft OAuth2 login page.
    ---
    tags: [Auth]
    responses:
      302:
        description: Redirect to Microsoft login
    """
    params = urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "response_mode": "query",
    })
    return redirect(f"{AUTH_BASE}/authorize?{params}")


@app.get("/auth/callback")
def callback():
    """
    OAuth2 callback — Microsoft redirects here after login. Do not call manually.
    ---
    tags: [Auth]
    parameters:
      - name: code
        in: query
        type: string
      - name: error
        in: query
        type: string
    responses:
      200:
        description: Authenticated successfully
      400:
        description: OAuth error or missing code
    """
    code = request.args.get("code")
    error = request.args.get("error")
    if error:
        return jsonify({"error": error, "description": request.args.get("error_description")}), 400
    if not code:
        return jsonify({"error": "missing code"}), 400

    resp = requests.post(f"{AUTH_BASE}/token", data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    })
    if not resp.ok:
        log.error("Token exchange failed [%s]: %s", resp.status_code, resp.text)
        return jsonify({"error": "token exchange failed", "details": resp.json()}), 400

    data = resp.json()
    _store_token(data["access_token"], data.get("refresh_token"), data.get("expires_in", 3600))
    return jsonify({"message": "authenticated", "expires_in": data.get("expires_in")})


@app.get("/auth/logout")
def logout():
    """
    Clear local session and sign out from Microsoft.
    ---
    tags: [Auth]
    parameters:
      - name: redirect
        in: query
        type: string
        description: "URL to redirect after logout (default: /auth/login)"
    responses:
      302:
        description: Redirect to Microsoft logout
    """
    _save_tokens({})
    session.clear()
    post_logout = request.args.get("redirect", url_for("login", _external=True))
    params = urlencode({"post_logout_redirect_uri": post_logout})
    return redirect(f"{AUTH_BASE}/logout?{params}")


# ── User / file routes ────────────────────────────────────────────────────────

@app.get("/me")
def me():
    """
    Get the currently authenticated user's profile.
    ---
    tags: [User]
    responses:
      200:
        description: User profile
        schema:
          properties:
            id: {type: string}
            display_name: {type: string}
            email: {type: string}
      401:
        description: Not authenticated
    """
    try:
        resp = graph_get("/me")
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    data = resp.json()
    return jsonify({"id": data.get("id"), "display_name": data.get("displayName"),
                    "email": data.get("mail") or data.get("userPrincipalName")})


@app.get("/files")
def list_root():
    """
    List files and folders in the OneDrive root.
    ---
    tags: [Files]
    responses:
      200:
        description: List of items in root
      401:
        description: Not authenticated
    """
    try:
        resp = graph_get("/me/drive/root/children",
                         params={"$select": "id,name,size,folder,file,lastModifiedDateTime,webUrl"})
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    return jsonify(_format_items(resp.json().get("value", [])))


@app.get("/files/<item_id>")
def list_folder(item_id):
    """
    List children of a folder by item ID.
    ---
    tags: [Files]
    parameters:
      - name: item_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: List of items in the folder
      401:
        description: Not authenticated
    """
    try:
        resp = graph_get(f"/me/drive/items/{item_id}/children",
                         params={"$select": "id,name,size,folder,file,lastModifiedDateTime,webUrl"})
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    return jsonify(_format_items(resp.json().get("value", [])))


@app.get("/files/<item_id>/info")
def file_info(item_id):
    """
    Get metadata for a single file or folder.
    ---
    tags: [Files]
    parameters:
      - name: item_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: File or folder metadata
      401:
        description: Not authenticated
    """
    try:
        resp = graph_get(f"/me/drive/items/{item_id}")
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    return jsonify(_format_item(resp.json()))


@app.get("/files/<item_id>/download-url")
def download_url(item_id):
    """
    Get a short-lived direct download URL for a file.
    ---
    tags: [Files]
    parameters:
      - name: item_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Download URL
        schema:
          properties:
            id: {type: string}
            name: {type: string}
            download_url: {type: string}
      400:
        description: Item is not a file
      401:
        description: Not authenticated
    """
    try:
        resp = graph_get(f"/me/drive/items/{item_id}",
                         params={"$select": "id,name,@microsoft.graph.downloadUrl"})
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    item = resp.json()
    url = item.get("@microsoft.graph.downloadUrl")
    if not url:
        return jsonify({"error": "not a file or download URL unavailable"}), 400
    return jsonify({"id": item["id"], "name": item["name"], "download_url": url})


@app.get("/search")
def search_files():
    """
    Search OneDrive files by name.
    ---
    tags: [Files]
    parameters:
      - name: q
        in: query
        type: string
        required: true
        description: Search query
    responses:
      200:
        description: Matching files and folders
      400:
        description: Missing query param
      401:
        description: Not authenticated
    """
    q = request.args.get("q", "")
    if not q:
        return jsonify({"error": "missing query param 'q'"}), 400
    try:
        resp = graph_get(f"/me/drive/root/search(q='{q}')",
                         params={"$select": "id,name,size,folder,file,lastModifiedDateTime,webUrl"})
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    return jsonify(_format_items(resp.json().get("value", [])))


# ── Graph subscription routes ─────────────────────────────────────────────────

@app.post("/subscriptions")
def create_subscription():
    """
    Create a Graph change notification subscription on OneDrive.
    ---
    tags: [Subscriptions]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          required: [notification_url]
          properties:
            notification_url:
              type: string
              description: Public HTTPS URL pointing to /onedrive/notifications
              example: https://abc.trycloudflare.com/onedrive/notifications
            resource:
              type: string
              description: OneDrive Graph resource to watch
              default: /me/drive/root
            change_types:
              type: string
              description: Change type — OneDrive drive resource supports 'updated' only
              default: updated
            expiration_hours:
              type: integer
              description: Hours until subscription expires (max ~4230)
              default: 1
            client_state:
              type: string
              description: Optional secret echoed back in notifications
    responses:
      201:
        description: Subscription created
      400:
        description: Missing notification_url
      401:
        description: Not authenticated
    """
    body = request.get_json(force=True) or {}
    notification_url = body.get("notification_url") or body.get("notificationUrl")
    if not notification_url:
        return jsonify({"error": "notification_url is required"}), 400

    resource = body.get("resource", "/me/drive/root")
    change_types = body.get("change_types") or body.get("changeType", "updated")
    hours = int(body.get("expiration_hours", 1))
    expiration = (datetime.now(timezone.utc) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    client_state = body.get("client_state") or body.get("clientState", "")

    payload = {
        "changeType": change_types,
        "notificationUrl": notification_url,
        "resource": resource,
        "expirationDateTime": expiration,
        "clientState": client_state,
    }

    try:
        resp = graph_post("/subscriptions", json=payload)
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401

    if not resp.ok:
        return jsonify({"error": "subscription creation failed", "details": resp.json()}), resp.status_code
    return jsonify(resp.json()), 201


@app.get("/subscriptions")
def list_subscriptions():
    """
    List all active Graph change notification subscriptions.
    ---
    tags: [Subscriptions]
    responses:
      200:
        description: List of subscriptions
      401:
        description: Not authenticated
    """
    try:
        resp = graph_get("/subscriptions")
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    return jsonify(resp.json().get("value", []))


@app.delete("/subscriptions/<sub_id>")
def delete_subscription(sub_id):
    """
    Delete a Graph change notification subscription.
    ---
    tags: [Subscriptions]
    parameters:
      - name: sub_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Deleted
      401:
        description: Not authenticated
    """
    try:
        resp = graph_delete(f"/subscriptions/{sub_id}")
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if resp.status_code == 204:
        return jsonify({"message": "deleted"})
    return jsonify(resp.json()), resp.status_code


@app.post("/subscriptions/<sub_id>/renew")
def renew_subscription(sub_id):
    """
    Renew an expiring subscription.
    ---
    tags: [Subscriptions]
    parameters:
      - name: sub_id
        in: path
        type: string
        required: true
      - in: body
        name: body
        schema:
          properties:
            expiration_hours:
              type: integer
              default: 1
    responses:
      200:
        description: Subscription renewed
      401:
        description: Not authenticated
    """
    body = request.get_json(force=True) or {}
    hours = int(body.get("expiration_hours", 1))
    expiration = (datetime.now(timezone.utc) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = graph_patch(f"/subscriptions/{sub_id}", json={"expirationDateTime": expiration})
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    if not resp.ok:
        return jsonify(resp.json()), resp.status_code
    return jsonify(resp.json())


# ── Webhook endpoint ──────────────────────────────────────────────────────────

@app.route("/onedrive/notifications", methods=["GET", "POST"])
def onedrive_notifications():
    """
    Webhook endpoint called by Microsoft Graph for OneDrive change notifications.
    Handles validation handshake and publishes file events to Confluent Cloud Kafka.
    ---
    tags: [Webhook]
    parameters:
      - name: validationToken
        in: query
        type: string
        description: Present only during Graph subscription validation handshake
    responses:
      200:
        description: Validation token echoed back (handshake only)
      202:
        description: Notifications acknowledged
      400:
        description: Invalid JSON
    """
    # Step 1: Graph subscription validation handshake
    validation_token = request.args.get("validationToken")
    if validation_token:
        log.info("Graph subscription validation handshake received")
        return Response(validation_token, status=200, mimetype="text/plain")

    # Step 2: Process change notifications
    try:
        data = request.get_json(force=True)
    except Exception:
        return Response("Invalid JSON", status=400)

    notifications = data.get("value", []) if data else []
    log.info("Received %d notification(s) from Graph", len(notifications))
    log.info("Notification payload: %s", json.dumps(data))

    for n in notifications:
        change_type = n.get("changeType", "updated")
        try:
            # OneDrive drive subscriptions send resourceData=null.
            # Use delta API to discover what actually changed.
            _process_delta(change_type)
        except Exception as e:
            log.error("Error processing delta: %s", e)

    # Graph requires 202 acknowledgement within a few seconds
    return Response(status=202)


DELTA_TOKEN_FILE = "delta_token.json"
ITEM_STATE_FILE = "item_state.json"


def _load_delta_token() -> str | None:
    if os.path.exists(DELTA_TOKEN_FILE):
        with open(DELTA_TOKEN_FILE) as f:
            return json.load(f).get("token")
    return None


def _save_delta_token(token: str):
    with open(DELTA_TOKEN_FILE, "w") as f:
        json.dump({"token": token}, f)


def _load_item_state() -> dict:
    if os.path.exists(ITEM_STATE_FILE):
        with open(ITEM_STATE_FILE) as f:
            return json.load(f)
    return {}


def _save_item_state(state: dict):
    with open(ITEM_STATE_FILE, "w") as f:
        json.dump(state, f)


WATCH_RESOURCE = os.environ.get("WATCH_RESOURCE", "/me/drive/root")


def _process_delta(change_type: str):
    """
    Use the OneDrive delta API to get all items changed since the last call.
    Lock prevents concurrent webhook calls from fetching the same delta twice.
    """
    with _delta_lock:
        delta_token = _load_delta_token()
        is_bootstrap = delta_token is None

        url = (f"https://graph.microsoft.com/v1.0/me/drive/root/delta?$token={delta_token}"
               if delta_token else
               "https://graph.microsoft.com/v1.0/me/drive/root/delta")

        if is_bootstrap:
            log.info("No delta token found — bootstrapping item state (no events will be published)")

        item_state = _load_item_state()

        while url:
            resp = graph_get(url)
            if not resp.ok:
                log.error("Delta API error: %s", resp.text)
                return
            data = resp.json()

            for item in data.get("value", []):
                if "folder" in item or item.get("root") is not None:
                    continue

                item_id = item.get("id")

                if "deleted" in item:
                    prev = item_state.pop(item_id, {})
                    if not is_bootstrap:
                        parent_ref = item.get("parentReference", {})
                        enriched = {
                            **item,
                            "name": prev.get("name"),
                            "parentReference": {
                                "driveId": parent_ref.get("driveId"),
                                "id": prev.get("parent_id") or parent_ref.get("id"),
                                "path": prev.get("folder_path"),
                            },
                            "file": {"mimeType": prev.get("mime_type")},
                            "size": prev.get("size"),
                            "webUrl": prev.get("web_url"),
                        }
                        _publish_item(enriched, "deleted")
                    continue

                if WATCH_RESOURCE != "/me/drive/root":
                    folder_name = WATCH_RESOURCE.split(":/")[-1].strip("/")
                    item_path = item.get("parentReference", {}).get("path", "")
                    if folder_name.lower() not in item_path.lower():
                        if not is_bootstrap:
                            log.info("Skipping item outside watched folder: %s", item.get("name"))
                        continue

                current_name = item.get("name")
                current_parent_id = item.get("parentReference", {}).get("id")

                if not is_bootstrap:
                    prev = item_state.get(item_id)
                    if prev is None:
                        effective_type = "created"
                        prev_name = None
                        prev_folder_path = None
                    else:
                        prev_name = prev.get("name")
                        prev_folder_path = prev.get("folder_path")
                        name_changed = prev_name != current_name
                        parent_changed = prev.get("parent_id") != current_parent_id
                        if name_changed and parent_changed:
                            effective_type = "moved_renamed"
                        elif name_changed:
                            effective_type = "renamed"
                        elif parent_changed:
                            effective_type = "moved"
                        else:
                            effective_type = change_type

                item_state[item_id] = {
                    "name": current_name,
                    "parent_id": current_parent_id,
                    "folder_path": item.get("parentReference", {}).get("path"),
                    "mime_type": item.get("file", {}).get("mimeType"),
                    "size": item.get("size"),
                    "web_url": item.get("webUrl"),
                }

                if not is_bootstrap:
                    _publish_item(
                        item,
                        effective_type,
                        previous_name=prev_name if effective_type in ("renamed", "moved_renamed") else None,
                        previous_folder_path=prev_folder_path if effective_type in ("moved", "moved_renamed") else None,
                    )

            url = data.get("@odata.nextLink")

            delta_link = data.get("@odata.deltaLink", "")
            if delta_link:
                token = delta_link.split("$token=")[-1] if "$token=" in delta_link else delta_link
                _save_delta_token(token)

        _save_item_state(item_state)
        if is_bootstrap:
            log.info("Bootstrap complete — %d items in state, ready to process events", len(item_state))


def _publish_item(
    meta: dict,
    change_type: str,
    previous_name: str | None = None,
    previous_folder_path: str | None = None,
):
    item_id = meta.get("id")
    is_folder = "folder" in meta

    parent_ref = meta.get("parentReference", {})
    file_facet = meta.get("file", {})
    folder_facet = meta.get("folder", {})
    last_modified_by = meta.get("lastModifiedBy", {})
    created_by = meta.get("createdBy", {})
    sharepoint_ids = meta.get("sharepointIds", {})
    item_kind = "folder" if is_folder else "file"

    record = {
        # ── Event envelope ──────────────────────────────────────────
        "event_type": f"onedrive.{item_kind}." + change_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "microsoft-graph/onedrive",

        # ── What ────────────────────────────────────────────────────
        "onedrive_item_id": item_id,
        "name": meta.get("name"),
        "previous_name": previous_name,
        "mime_type": file_facet.get("mimeType"),
        "size_bytes": meta.get("size"),
        "child_count": folder_facet.get("childCount"),
        "web_url": meta.get("webUrl"),
        "etag": meta.get("eTag"),
        "ctag": meta.get("cTag"),
        "file_hash": file_facet.get("hashes", {}).get("quickXorHash"),

        # ── Where ───────────────────────────────────────────────────
        "drive_id": parent_ref.get("driveId"),
        "parent_id": parent_ref.get("id"),
        "folder_path": parent_ref.get("path"),
        "previous_folder_path": previous_folder_path,

        # ── When ────────────────────────────────────────────────────
        "created_at": meta.get("createdDateTime"),
        "last_modified_at": meta.get("lastModifiedDateTime"),

        # ── Who (modifier) ──────────────────────────────────────────
        "modified_by_name": last_modified_by.get("user", {}).get("displayName"),
        "modified_by_email": last_modified_by.get("user", {}).get("email"),
        "modified_by_id": last_modified_by.get("user", {}).get("id"),

        # ── Who (creator) ───────────────────────────────────────────
        "created_by_name": created_by.get("user", {}).get("displayName"),
        "created_by_email": created_by.get("user", {}).get("email"),
        "created_by_id": created_by.get("user", {}).get("id"),

        # ── SharePoint context ───────────────────────────────────────
        "sharepoint_site_id": sharepoint_ids.get("siteId"),
        "sharepoint_list_id": sharepoint_ids.get("listId"),
        "sharepoint_list_item_id": sharepoint_ids.get("listItemId"),
    }

    log.info("Publishing event for file: %s (%s)", meta.get("name"), item_id)
    send_to_kafka(item_id, record)


# ── Formatters ────────────────────────────────────────────────────────────────

def _format_item(item: dict) -> dict:
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "type": "folder" if "folder" in item else "file",
        "size": item.get("size"),
        "mime_type": item.get("file", {}).get("mimeType") if "file" in item else None,
        "last_modified": item.get("lastModifiedDateTime"),
        "web_url": item.get("webUrl"),
        "child_count": item.get("folder", {}).get("childCount") if "folder" in item else None,
    }


def _format_items(items: list) -> list:
    return [_format_item(i) for i in items]


if __name__ == "__main__":
    app.run(debug=True, port=5000)
