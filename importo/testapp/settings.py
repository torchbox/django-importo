import os

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ALLOWED_HOSTS = ["*"]

DATABASES = {
    "default": {
        "NAME": "importo-test.sqlite",
        "ENGINE": "django.db.backends.sqlite3",
    }
}

INSTALLED_APPS = (
    "importo",
    "importo.testapp",
    "modelcluster",
    "taggit",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.sites",
    "wagtail.admin",
    "wagtail.core",
    "wagtail.sites",
    "wagtail.search",
    "wagtail.users",
    "wagtail.images",
    "wagtail.documents",
    "wagtail.contrib.frontend_cache",
    "wagtail.contrib.modeladmin",
)

ROOT_URLCONF = "importo.testapp.urls"
SECRET_KEY = "fake-key"

# Django i18n
TIME_ZONE = "Europe/London"
USE_TZ = True

# Used in Wagtail emails
BASE_URL = "https://localhost:8000"

# Don't redirect to HTTPS in tests
SECURE_SSL_REDIRECT = False

# By default, Django uses a computationally difficult algorithm for passwords hashing.
# We don't need such a strong algorithm in tests, so use MD5
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
