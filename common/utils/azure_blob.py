import datetime
import json
import time
import traceback
import joblib
import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.policies import RetryPolicy
from azure.core.pipeline.transport import RequestsTransport
from pathlib import Path
import os

# Azure Blob configuration
AZURE_ACCOUNT_URL = "https://streamlinestorageblob.blob.core.windows.net"
AZURE_CONTAINER_NAME = "recs-artifacts"
AZURE_CREDENTIAL = os.getenv("AZURE_CREDENTIAL")

# Artifacts to load (blob path → local filename)
ARTIFACTS = {
    "tfidf_vectorizer": "latest/tfidf_vectorizer.pkl",
    "item_feature_matrix": "latest/item_feature_matrix.npy",
    "item_feature_matrix_movie_id_lookup": "latest/item_feature_matrix_movie_id_lookup.json",
    "baseline_recs": "latest/baseline_recommendations.parquet",
    "movie_features": "latest/movie_features.npy",
    "movie_features_movie_id_lookup": "latest/movie_features_movie_id_lookup.json",
    "movies_metadata": "latest/movies_metadata.parquet",
}

# Local cache directory
CACHE_DIR = Path("artifacts")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

blob_service_client = BlobServiceClient(
    account_url=AZURE_ACCOUNT_URL,
    credential=AZURE_CREDENTIAL,
)


def download_blob_to_cache(blob_path: str, local_path: Path):
    blob_client = blob_service_client.get_blob_client(
        container=AZURE_CONTAINER_NAME, blob=blob_path
    )
    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())
    print(f"📥 Downloaded: {blob_path} → {local_path}")


def upload_file_to_blob(local_path: Path, blob_path: str, retries=3):
    for attempt in range(retries):
        try:
            blob_client = blob_service_client.get_blob_client(
                container=AZURE_CONTAINER_NAME, blob=blob_path
            )
            with open(local_path, "rb") as f:
                blob_client.upload_blob(
                    f,
                    overwrite=True,
                    max_concurrency=2,
                    timeout=600,
                    connection_timeout=600,
                )
            print(f"Uploaded: {local_path} → {blob_path}")
            return
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                print(f"Failed to upload file {local_path} after {retries} attempts")
                print(traceback.format_exc())


def load_artifacts(force_refresh=False):
    artifacts = {}

    for key, blob_path in ARTIFACTS.items():
        filename = Path(blob_path).name
        local_path = CACHE_DIR / filename

        if force_refresh or not local_path.exists():
            print(f"⬇️ Downloading {key} from Azure Blob...")
            download_blob_to_cache(blob_path, local_path)
        else:
            print(f"✅ Using cached {key} from {local_path}")

        # Load into memory
        if filename.endswith(".pkl"):
            artifacts[key] = joblib.load(local_path)
        elif filename.endswith(".npy"):
            artifacts[key] = np.load(local_path)
        elif filename.endswith(".json"):
            with open(local_path) as f:
                artifacts[key] = json.load(f)
        elif filename.endswith(".parquet"):
            artifacts[key] = pd.read_parquet(local_path)

    print("🚀 All artifacts ready.")
    return artifacts


def save_and_upload_artifact(key: str, data, version: str = "latest"):
    """
    Save and upload an artifact. `key` must match keys in ARTIFACTS.
    """
    if key not in ARTIFACTS:
        raise ValueError(f"Unknown artifact key: {key}")

    filename = Path(ARTIFACTS[key]).name
    blob_path = f"{version}/{filename}"
    local_path = CACHE_DIR / filename

    # Save locally
    if filename.endswith(".pkl"):
        joblib.dump(data, local_path)
    elif filename.endswith(".npy"):
        np.save(local_path, data)
    elif filename.endswith(".json"):
        with open(local_path, "w") as f:
            json.dump(data, f)
    elif filename.endswith(".parquet"):
        data.to_parquet(local_path)
    else:
        raise ValueError("Unsupported file type for saving")

    # Upload to Blob
    upload_file_to_blob(local_path, blob_path)

    print(f"✅ {key} saved & uploaded to Azure Blob.")


def save_all_artifacts(
    tfidf_vectorizer,
    item_feature_matrix,
    item_feature_matrix_movie_id_lookup,
    baseline_recs,
    movie_features,
    movie_features_movie_id_lookup,
    movies_metadata,
    version=None,
):
    # Use current date if no version provided
    if version is None:
        version = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d")

    print(f"📦 Saving and uploading artifacts under version: {version}")

    save_and_upload_artifact("tfidf_vectorizer", tfidf_vectorizer, version=version)
    save_and_upload_artifact(
        "item_feature_matrix", item_feature_matrix, version=version
    )
    save_and_upload_artifact(
        "item_feature_matrix_movie_id_lookup",
        item_feature_matrix_movie_id_lookup,
        version=version,
    )
    save_and_upload_artifact("baseline_recs", baseline_recs, version=version)
    save_and_upload_artifact(
        "movie_features_movie_id_lookup",
        movie_features_movie_id_lookup,
        version=version,
    )

    save_and_upload_artifact(
        "movies_metadata",
        movies_metadata,
        version=version,
    )
    save_and_upload_artifact("movie_features", movie_features, version=version)
