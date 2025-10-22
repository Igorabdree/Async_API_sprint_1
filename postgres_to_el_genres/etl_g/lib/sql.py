"""SQL query templates."""

get_modified_records = """
    SELECT id, updated_at as modified FROM {table}
    WHERE updated_at > %(modified)s
    ORDER BY updated_at
    LIMIT %(page_size)s
"""

get_genres_info_by_id = """
    SELECT
        g.id AS genre_id,
        g.pname AS genre_name,
        ARRAY_AGG(fw.title) AS film_titles,
        ARRAY_AGG(fw.id) AS film_ids,
        COUNT(fw.id) AS films_count
    FROM content.genre g  -- ✅ Добавлена схема content
    INNER JOIN content.genre_film_work gfw ON g.id = gfw.genre_id
    INNER JOIN content.film_work fw ON gfw.film_work_id = fw.id
    GROUP BY g.id, g.pname
    ORDER BY g.pname
"""