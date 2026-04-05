import os
import firebase_admin
from firebase_admin import credentials, firestore, auth
from backend.logger import get_logger

logger = get_logger(__name__)

_app = None
_db = None


def initialize_firebase():
    global _app, _db
    if _app:
        return _app, _db

    cred_path = os.getenv(
        "FIREBASE_CREDENTIALS_PATH",
        os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
    )
    project_id = os.getenv("FIREBASE_PROJECT_ID", "spark-cloud-analytics-38260")

    try:
        if os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            _app = firebase_admin.initialize_app(cred)
            logger.info("Firebase initialized with service account credentials.")
        elif project_id:
            _app = firebase_admin.initialize_app(options={"projectId": project_id})
            logger.info("Firebase initialized with Application Default Credentials.")
        else:
            raise RuntimeError("No Firebase credentials found. Set FIREBASE_CREDENTIALS_PATH or FIREBASE_PROJECT_ID.")

        _db = firestore.client()
        return _app, _db
    except Exception as e:
        logger.error(f"Firebase initialization failed: {e}")
        raise


def get_db():
    global _db
    if _db is None:
        initialize_firebase()
    return _db


def verify_token(id_token: str) -> dict:
    try:
        decoded = auth.verify_id_token(id_token)
        return decoded
    except Exception as e:
        logger.warning(f"Token verification failed: {e}")
        raise ValueError(f"Invalid or expired token: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# FIRESTORE PATH HELPERS
# All user data lives under: users/{uid}/...
# This is the SINGLE canonical place defining the hierarchy.
# ═══════════════════════════════════════════════════════════════════════════════

def user_ref(uid: str):
    """Document reference: users/{uid}"""
    return get_db().collection("users").document(uid)


def jobs_col(uid: str):
    """Subcollection reference: users/{uid}/jobs"""
    return user_ref(uid).collection("jobs")


def job_results_col(uid: str):
    """Subcollection reference: users/{uid}/job_results"""
    return user_ref(uid).collection("job_results")


# ═══════════════════════════════════════════════════════════════════════════════
# USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def get_user_role(uid: str) -> str:
    """Read the user's role from users/{uid}. Defaults to 'user'."""
    doc = user_ref(uid).get()
    if doc.exists:
        return doc.to_dict().get("role", "user")
    return "user"


def upsert_user(uid: str, email: str, display_name: str = "", provider: str = "email", role: str = "user"):
    """
    Create or update the user document at users/{uid}.

    - First login:    writes uid, email, display_name, provider, role, created_at, last_login
    - Return logins:  updates only email, display_name, provider, last_login
                      (never overwrites role or created_at)

    Structure in Firestore:
        users/
          {uid}/                  ← this document
            uid, email, role, created_at, last_login, ...
            jobs/                 ← subcollection (managed by job_controller)
            job_results/          ← subcollection (managed by job_controller)
    """
    from datetime import datetime, timezone

    ref = user_ref(uid)
    existing = ref.get()
    now = datetime.now(timezone.utc).isoformat()

    if not existing.exists:
        # ── Brand-new user ─────────────────────────────────────────────────────
        ref.set({
            "uid": uid,
            "email": email,
            "display_name": display_name or email.split("@")[0],
            "provider": provider,
            "role": role,
            "created_at": now,
            "last_login": now,
        })
        logger.info(f"New user stored in Firestore: {uid} ({email})")
    else:
        # ── Returning user — only update mutable fields ────────────────────────
        ref.update({
            "email": email,
            "display_name": display_name or email.split("@")[0],
            "provider": provider,
            "last_login": now,
        })
        logger.info(f"Returning user updated in Firestore: {uid} ({email})")