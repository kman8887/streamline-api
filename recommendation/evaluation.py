import pandas as pd


def evaluate_topk_metrics(pred_df: pd.DataFrame, test_df: pd.DataFrame, k: int = 100):
    users = test_df["user_id"].unique()

    total_hits = 0
    total_precision = 0
    total_recall = 0

    for user_id in users:
        user_recs = (
            pred_df[pred_df["user_id"] == str(user_id)]
            .sort_values("final_score", ascending=False)
            .head(k)
        )

        recommended = set(user_recs["movie_id"])
        relevant = set(test_df[test_df["user_id"] == user_id]["movie_id"])

        if not relevant:
            continue

        print(str(len(relevant)) + "test data")
        hits = len(recommended & relevant)

        # Per-user metrics
        precision = hits / k
        recall = hits / len(relevant) if relevant else 0

        total_hits += int(hits > 0)
        total_precision += precision
        total_recall += recall

    print(total_hits)
    n_users = len(users)
    return {
        "Hit@K": round(total_hits / n_users, 4),
        "Precision@K": round(total_precision / n_users, 4),
        "Recall@K": round(total_recall / n_users, 4),
    }
