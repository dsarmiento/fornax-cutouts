from typing import Any


def get_pagination_metadata(
    page: int,
    limit: int,
    total_items: int,
    base_url: str,
) -> dict[str, dict[str, Any]]:
    """
    Generate pagination metadata for a paginated response.
    """
    total_pages = (total_items + limit - 1) // limit

    metadata = {
        "page": page,
        "limit": limit,
        "totalItems": total_items,
        "totalPages": total_pages,
        "currentPage": page + 1
    }

    links = {
        "self": f"{base_url}?page={page}&limit={limit}",
        "first": f"{base_url}?page=0&limit={limit}",
        "last": f"{base_url}?page={total_pages - 1}&limit={limit}"
    }
    if page > 0:
        links["prev"] = f"{base_url}?page={page - 1}&limit={limit}"
    if page < total_pages - 1:
        links["next"] = f"{base_url}?page={page + 1}&limit={limit}"

    return {
        "metadata": metadata,
        "links": links
    }
