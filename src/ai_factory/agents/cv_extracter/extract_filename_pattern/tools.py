def convert_to_percentage(entity_counts: dict) -> dict:
    """
    Retrieves the volume percentage of each vendor based on the total counts and division.

    Args:
        entity_counts (dict): The entity dict with the format of vendor -> count

    Returns:
        dict: A dictionary with the same keys with their count in percentage instead of absolute
    """
    if not entity_counts:
        return {}

    total = sum(entity_counts.values())
    if total == 0:
        # Avoid division by zero
        return {k: 0.0 for k in entity_counts}

    return {k: (v / total) * 100 for k, v in entity_counts.items()}
