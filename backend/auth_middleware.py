from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from backend.firebase_config import verify_token, get_user_role, upsert_user
from backend.logger import get_logger

logger = get_logger(__name__)
bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    """
    Verify the Firebase ID token and return user info.

    On every successful verification:
      1. Decode the token → get uid, email, name, provider
      2. Call upsert_user() → ensures users/{uid} exists in Firestore
         (creates on first login, updates last_login on return visits)
      3. Read the user's role from Firestore
      4. Return { uid, email, display_name, provider, role }
    """
    token = credentials.credentials
    try:
        decoded = verify_token(token)
    except Exception as e:
        logger.warning(f"Auth failed: {e}")
        raise HTTPException(status_code=401, detail="Invalid or expired authentication token.")

    uid          = decoded["uid"]
    email        = decoded.get("email", "")
    display_name = decoded.get("name", "")        # present for Google sign-in
    provider     = _extract_provider(decoded)

    # ── Guarantee the user document exists in Firestore ──────────────────────
    # This is the single point that ensures every authenticated user is stored.
    try:
        upsert_user(uid=uid, email=email, display_name=display_name, provider=provider)
    except Exception as e:
        # Log but don't block the request — auth is still valid
        logger.error(f"Failed to upsert user {uid} in Firestore: {e}")

    role = get_user_role(uid)

    return {
        "uid": uid,
        "email": email,
        "display_name": display_name,
        "provider": provider,
        "role": role,
    }


def _extract_provider(decoded: dict) -> str:
    """Determine the sign-in provider from the decoded token."""
    firebase_info = decoded.get("firebase", {})
    sign_in_provider = firebase_info.get("sign_in_provider", "")
    if "google" in sign_in_provider:
        return "google"
    if "password" in sign_in_provider:
        return "email"
    return sign_in_provider or "unknown"


def require_role(*roles: str):
    """Dependency factory: ensures the current user has one of the given roles."""
    async def checker(user: dict = Depends(get_current_user)):
        if user["role"] not in roles:
            raise HTTPException(
                status_code=403,
                detail=f"Access denied. Required role(s): {list(roles)}",
            )
        return user
    return checker