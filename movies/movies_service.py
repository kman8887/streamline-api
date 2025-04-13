from collections import OrderedDict
from dataclasses import asdict
from typing import Dict, List, Optional
from flask import g, request
from psycopg.rows import dict_row
import psycopg
from common.utils.utils import DB_CONFIG, time_it
from movies.model.filter_option import FilterOption
from movies.model.filter_options import FilterOptions
from movies.model.genre import Genre
from movies.model.movie import Movie
from movies.model.movies_data import MovieData, WatchlistMovie
from movies.model.movies_filter_params import MoviesFilterParams
from movies.model.onboarding_movie import OnboardingMovie
from movies.model.tag import Tag
from movies.model.user_movie_interactions import MovieDetailsUserInteraction
from movies.model.watch_provider import WatchProvider
from recommendation.model.user_movie_interaction import MovieMetadata
import users.users_service as users_service
from common.utils.utils import cache
import hashlib
import json


def get_filter_params(logged_in_user: int) -> MoviesFilterParams:
    page_start, page_size = __get_paging_params()

    watch_providers = (
        [int(x) for x in request.args.getlist("watchProviders")]
        if request.args.getlist("watchProviders")
        else []
    )

    if logged_in_user is not None:
        order_by, order_direction = get_movie_sorting("predicted_score")
        if request.args.get("onlyShowUsersWatchProviders") == "true":
            users_watch_providers = users_service.getUserWatchProviders(logged_in_user)

            watch_providers = list(
                set(users_watch_providers or []) | set(watch_providers)
            )
    else:
        order_by, order_direction = get_movie_sorting()

    return MoviesFilterParams(
        search=request.args.get("search"),
        genre_ids=(
            request.args.getlist("genre", type=int)
            if request.args.getlist("genre")
            else None
        ),
        tag_ids=(
            [int(x) for x in request.args.getlist("tags")]
            if request.args.getlist("tags")
            else None
        ),
        release_date_from=request.args.get("release_date_from"),
        release_date_to=request.args.get("release_date_to"),
        rating_from=request.args.get("ratingFrom", type=float),
        rating_to=request.args.get("ratingTo", type=float),
        watch_provider_ids=(watch_providers if watch_providers else None),
        region_filter=g.region,
        status_filter=request.args.get("status"),
        languages=g.languages,
        order_by=order_by,
        order_direction=order_direction,
        limit_rows=page_size,
        offset_rows=page_start,
    )


def get_movie_sorting(default_sorting="popularity"):
    if request.args.get("sort"):
        sortArg = request.args.get("sort")

        if sortArg.find(":") > -1:
            field, direction = sortArg.split(":")
            return field, "DESC" if direction == "-1" else "ASC"
        else:
            return sortArg, "ASC"

    return default_sorting, "DESC"


def get_movies_and_count_cached(
    params: MoviesFilterParams, include_user: bool = True
) -> Dict[str, List[Dict[str, any]]]:
    cache_key = __generate_movies_cache_key(params, include_user=include_user)
    cached_result = cache.get(cache_key)

    if cached_result:
        print("cached result")
        return cached_result

    result = __get_movies_and_count(params)
    cache.set(cache_key, result, timeout=3600)

    return result


def __get_movies_and_count(
    params: MoviesFilterParams,
) -> Dict[str, List[Dict[str, any]]]:
    select_query = """
                SELECT * FROM get_filtered_movies(
                    %(search)s, %(genre_ids)s, %(tag_ids)s,
                    %(release_date_from)s, %(release_date_to)s,
                    %(rating_from)s::numeric, %(rating_to)s::numeric, %(watch_provider_ids)s,
                    %(region_filter)s, %(status_filter)s,
                    %(languages)s,
                    %(order_by)s, %(order_direction)s,
                    %(limit_rows)s, %(offset_rows)s
                )
                """

    select_query_with_recommendation = """
                SELECT * FROM get_filtered_movies_with_recommendation_score(
                    %(user_id_param)s, %(search)s, %(genre_ids)s, %(tag_ids)s,
                    %(release_date_from)s, %(release_date_to)s,
                    %(rating_from)s::numeric, %(rating_to)s::numeric, %(watch_provider_ids)s,
                    %(region_filter)s, %(status_filter)s,
                    %(languages)s,
                    %(order_by)s, %(order_direction)s,
                    %(limit_rows)s, %(offset_rows)s
                )
                """

    select_recommendations_query = """
        SELECT COUNT(*) AS count
        FROM user_recommendations ur
        WHERE ur.user_id = %(user_id_param)s
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if params.user_id_param is not None:
                cur.execute(
                    (select_recommendations_query),
                    asdict(params),
                )

                recommendation_count = cur.fetchone()["count"]

                if recommendation_count == 0:
                    cur.execute(
                        (select_query),
                        asdict(params),
                    )
                else:
                    cur.execute(
                        (select_query_with_recommendation),
                        asdict(params),
                    )
            else:
                cur.execute(
                    (select_query),
                    asdict(params),
                )

            movies = [MovieData(**row) for row in cur.fetchall()]

            # ✅ Call `count_filtered_movies()`
            cur.execute(
                """
                SELECT count_filtered_movies(
                    %(search)s, %(genre_ids)s, %(tag_ids)s,
                    %(release_date_from)s, %(release_date_to)s,
                    %(rating_from)s::numeric, %(rating_to)s::numeric, %(watch_provider_ids)s,
                    %(region_filter)s, %(status_filter)s, %(languages)s
                )
                """,
                asdict(params),
            )
            total_count: int = cur.fetchone()["count_filtered_movies"]

    return {"movies": [asdict(movie) for movie in movies], "total_count": total_count}


@cache.memoize(timeout=3600)
def get_distinct_genres_tags_and_watch_providers(region: str) -> FilterOptions:
    """Fetch distinct genres and tags from the database."""
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # ✅ Fetch distinct genres
            cur.execute("SELECT id, name FROM genres ORDER BY name")
            genres = [FilterOption(**row) for row in cur.fetchall()]

            # ✅ Fetch distinct tags
            cur.execute(
                "SELECT id, name FROM tags t INNER JOIN movie_tags mt ON t.id = mt.tag_id GROUP BY t.id ORDER BY COUNT(t.id) DESC;"
            )
            tags = [FilterOption(**row) for row in cur.fetchall()]

            cur.execute(
                "SELECT wp.id, wp.provider_name as name FROM watch_providers wp INNER JOIN watch_provider_regions wpr ON wp.id = wpr.provider_id WHERE wpr.region = %s ORDER BY wpr.priority ASC;",
                [region],
            )
            watch_providers = [FilterOption(**row) for row in cur.fetchall()]

    return FilterOptions(genres=genres, tags=tags, watch_providers=watch_providers)


@cache.memoize(timeout=3600)
def get_distinct_watch_providers(region: str) -> List[FilterOption]:
    """Fetch distinct watch providers from the database."""
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT wp.id, wp.provider_name as name FROM watch_providers wp INNER JOIN watch_provider_regions wpr ON wp.id = wpr.provider_id WHERE wpr.region = %s ORDER BY wpr.priority ASC;",
                [region],
            )
            watch_providers = [FilterOption(**row) for row in cur.fetchall()]

    return watch_providers


def get_onboarding_movies(user_id: int) -> OnboardingMovie:
    page_start, page_size = __get_paging_params()

    print(f"page_start: {page_start}, page_size: {page_size}")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Fetch onboarding movies
            cur.execute(
                """
                SELECT movies.id, title, release_date, poster_path
                FROM movies 
                LEFT JOIN user_movie_interactions umi 
                    ON movies.id = umi.movie_id 
                    AND umi.user_id = %s 
                    AND umi.interaction_type = 'RATING' 
                    AND umi.active = true
                WHERE umi.id IS NULL
                AND interaction_count IS NOT NULL
                ORDER BY interaction_count DESC 
                OFFSET %s LIMIT 12;
                """,
                [user_id, page_start],
            )

            onboarding_movies = [OnboardingMovie(**row) for row in cur.fetchall()]

    return onboarding_movies


def get_movie_details(
    movie_id: str, region: str = "GB", user_id: Optional[int] = None
) -> Optional[Movie]:
    query = """
    WITH movie_data AS (
        SELECT 
            m.id, m.title, m.original_language, m.overview, m.tagline,
            m.runtime, m.backdrop_path,
            m.status, m.release_date, m.vote_average, m.vote_count, m.interaction_count
        FROM movies m
        WHERE m.id = %(movie_id)s
    ),
    tags AS (
        SELECT t.id, t.name 
        FROM tags t
        JOIN movie_tags mt ON t.id = mt.tag_id
        WHERE mt.movie_id = %(movie_id)s
    ),
    genres AS (
        SELECT g.id, g.name 
        FROM genres g
        JOIN movie_genres mg ON g.id = mg.genre_id
        WHERE mg.movie_id = %(movie_id)s
    ),
    watch_providers AS (
        SELECT 
            wp.id, wp.provider_name, wp.logo_path, 
            wpr.priority, mwp.type
        FROM movie_watch_providers mwp
        JOIN watch_providers wp ON mwp.provider_id = wp.id
        JOIN watch_provider_regions wpr ON mwp.provider_id = wpr.provider_id
        WHERE mwp.movie_id = %(movie_id)s AND mwp.region = %(region)s AND wpr.region = %(region)s
        ORDER BY wpr.priority ASC
    )
    {user_interactions_cte}
    
    SELECT 
        md.id, md.title, md.original_language, md.overview, md.tagline, md.runtime, md.backdrop_path,
        md.status, md.release_date, md.vote_average, md.vote_count, md.interaction_count, 
        COALESCE(jsonb_agg(DISTINCT jsonb_build_object('id', t.id, 'name', t.name)) FILTER (WHERE t.id IS NOT NULL), '[]') AS tags,
        COALESCE(jsonb_agg(DISTINCT jsonb_build_object('id', g.id, 'name', g.name)) FILTER (WHERE g.id IS NOT NULL), '[]') AS genres,
        COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
            'id', wp.id, 'name', wp.provider_name, 'logo_path', wp.logo_path, 
            'priority', wp.priority, 'type', wp.type
        )) FILTER (WHERE wp.id IS NOT NULL), '[]') AS watch_providers,
        -- Director (can be more than one, but usually one) So we get multiple
        (SELECT array_agg(c.name) 
        FROM movie_crew mc 
        JOIN credits c ON mc.credit_id = c.id 
        WHERE mc.movie_id = md.id AND mc.job = 'Director') AS director,
        
        (SELECT array_agg(c.name) 
        FROM movie_crew mc 
        JOIN credits c ON mc.credit_id = c.id 
        WHERE mc.movie_id = md.id AND mc.job IN ('Writer', 'Screenplay')) AS writer,

        -- Top 3 cast (ordered by cast_order)
        (SELECT array_agg(cast_sub.name)
            FROM (
                SELECT c.name
                FROM movie_cast mc
                JOIN credits c ON mc.credit_id = c.id
                WHERE mc.movie_id = md.id
                ORDER BY mc.cast_order ASC
                LIMIT 3
            ) AS cast_sub
        )AS top_3_cast
        {user_watch_list_select}
        {user_interactions_select}
    FROM movie_data md
    LEFT JOIN tags t ON TRUE
    LEFT JOIN genres g ON TRUE
    LEFT JOIN watch_providers wp ON TRUE
    {watch_list_join}
    {user_interactions_join}
    GROUP BY md.id, md.title, md.original_language, md.overview, md.tagline, md.runtime, md.backdrop_path,
        md.status, md.release_date, md.vote_average, md.vote_count, md.interaction_count
    """.format(
        user_interactions_cte=(
            """
        , user_interactions AS (
            SELECT 
                umi.id, umi.interaction_type, umi.rating, umi.created_at
            FROM user_movie_interactions umi
            WHERE umi.active = true AND umi.movie_id = %(movie_id)s AND umi.user_id = %(user_id)s
        )"""
            if user_id
            else ""
        ),
        user_watch_list_select=(
            """
            , CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM user_movie_list uml 
                    WHERE uml.movie_id = md.id AND uml.user_id = %(user_id)s
                ) THEN true
                ELSE false
            END AS is_movie_in_watchlist
            """
            if user_id
            else ""
        ),
        user_interactions_select=(
            """
        , COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
            'id', ui.id,
            'type', ui.interaction_type, 'rating', ui.rating, 
            'created_at', ui.created_at
        )) FILTER (WHERE ui.interaction_type IS NOT NULL), '[]') AS user_interactions
        """
            if user_id
            else ""
        ),
        watch_list_join=(
            """
            LEFT JOIN user_movie_list uml ON uml.movie_id = md.id AND (%(user_id)s IS NOT NULL AND uml.user_id = %(user_id)s)
            """
            if user_id
            else ""
        ),
        user_interactions_join=(
            "LEFT JOIN user_interactions ui ON TRUE" if user_id else ""
        ),
    )

    params = {"movie_id": movie_id, "region": region}
    if user_id:
        params["user_id"] = user_id

    try:
        with psycopg.connect(**DB_CONFIG, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                print("query: " + query)
                cur.execute(query, params)
                row = cur.fetchone()
                if not row:
                    return None

                return Movie(
                    id=row["id"],
                    title=row["title"],
                    original_language=row["original_language"],
                    overview=row["overview"],
                    tagline=row["tagline"],
                    runtime=row["runtime"],
                    backdrop_path=row["backdrop_path"],
                    status=row["status"],
                    release_date=row["release_date"],
                    vote_average=row["vote_average"],
                    vote_count=row["vote_count"],
                    tags=[Tag(**t) for t in row["tags"]],
                    genres=[Genre(**g) for g in row["genres"]],
                    watch_providers=[
                        WatchProvider(**wp) for wp in row["watch_providers"]
                    ],
                    director=[str(d) for d in row["director"]],
                    writer=[str(w) for w in row["writer"]],
                    top_cast=[str(c) for c in row["top_3_cast"]],
                    is_movie_in_watchlist=(
                        row["is_movie_in_watchlist"] if user_id else None
                    ),
                    user_interactions=(
                        [
                            MovieDetailsUserInteraction(**ui)
                            for ui in row["user_interactions"]
                        ]
                        if user_id
                        else None
                    ),
                )
    except Exception as e:
        print(f"Error fetching movie details: {e}")
        return None


@time_it
def get_movies_metadata() -> List[MovieMetadata]:
    query = """
    SELECT 
        id as movie_id, 
        popularity,
        vote_count,
        vote_average,
        title, 
        original_language, 
        CONCAT(FLOOR(EXTRACT(YEAR FROM m.release_date) / 10) * 10, 's') AS release_decade, 
        runtime, 
    (SELECT array_agg(g.name) FROM movie_genres mg JOIN genres g ON mg.genre_id = g.id WHERE mg.movie_id = m.id) AS genres,
    (SELECT array_agg(t.name) FROM movie_tags mt JOIN tags t ON mt.tag_id = t.id WHERE mt.movie_id = m.id) AS tags,
    
    -- Director (can be more than one, but usually one) So we get multiple
    (SELECT array_agg(c.name) 
     FROM movie_crew mc 
     JOIN credits c ON mc.credit_id = c.id 
     WHERE mc.movie_id = m.id AND mc.job = 'Director') AS director,
     
    (SELECT array_agg(c.name) 
     FROM movie_crew mc 
     JOIN credits c ON mc.credit_id = c.id 
     WHERE mc.movie_id = m.id AND mc.job IN ('Writer', 'Screenplay')) AS writer,

    -- Top 2 cast (ordered by cast_order)
    (SELECT array_agg(cast_sub.name)
  		FROM (
		    SELECT c.name
		    FROM movie_cast mc
		    JOIN credits c ON mc.credit_id = c.id
		    WHERE mc.movie_id = m.id
		    ORDER BY mc.cast_order ASC
		    LIMIT 2
		  ) AS cast_sub
     )AS top_2_cast
    FROM movies m
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # ✅ Fetch onboarding movies
            cur.execute(query)

            movies_metadata = [MovieMetadata(**row) for row in cur.fetchall()]

    return movies_metadata


def get_watchlist_movies(user_id) -> Dict[str, List[Dict[str, any]]]:
    page_start, page_size = __get_paging_params()

    query = """
     SELECT 
        m.id,
        title,
        vote_average,
        release_date,
        poster_path,
        backdrop_path,
        MAX(ur.predicted_score) AS recommendation_score
    FROM user_movie_list uml
    INNER JOIN movies m on m.id = uml.movie_id
    LEFT JOIN user_recommendations ur ON ur.movie_id = uml.movie_id AND ur.user_id = uml.user_id
    WHERE uml.user_id = %s
    GROUP BY m.id
    OFFSET %s LIMIT %s;
    """

    count_query = """
    SELECT COUNT(uml.movie_id)
    FROM user_movie_list uml
    WHERE uml.user_id = %s
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # ✅ Fetch onboarding movies
            cur.execute(query, [user_id, page_start, page_size])

            watchlist_movies = [WatchlistMovie(**row) for row in cur.fetchall()]

            cur.execute(count_query, [user_id])

            total_count: int = cur.fetchone()

    return {
        "movies": [asdict(movie) for movie in watchlist_movies],
        "total_count": total_count,
    }


def __get_paging_params():
    page_num = request.args.get("pn", type=int, default=0)
    page_size = request.args.get("ps", type=int, default=12)
    page_start = page_size * (page_num)

    return page_start, page_size


def __generate_movies_cache_key(
    params: MoviesFilterParams, include_user: bool = False
) -> str:
    key_data = {
        "search": params.search,
        "genre_ids": params.genre_ids,
        "tag_ids": params.tag_ids,
        "release_date_from": params.release_date_from,
        "release_date_to": params.release_date_to,
        "rating_from": params.rating_from,
        "rating_to": params.rating_to,
        "watch_provider_ids": params.watch_provider_ids,
        "region_filter": params.region_filter,
        "status_filter": params.status_filter,
        "languages": params.languages,
        "order_by": params.order_by,
        "order_direction": params.order_direction,
        "limit_rows": params.limit_rows,
        "offset_rows": params.offset_rows,
    }

    if include_user:
        key_data["user_id"] = params.user_id_param

    json_str = json.dumps(key_data, sort_keys=True)
    return "movies_filter:" + hashlib.sha256(json_str.encode("utf-8")).hexdigest()
