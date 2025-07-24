def abfss_to_https(abfss_path: str) -> str:
    """
    Convert an ABFSS path to an HTTPS URL.
    
    Example:
        abfss_path = "abfss://
    """
    import posixpath
    storage_account_endpoint = abfss_path.split('@')[1].split('/')[0]
    container = abfss_path.split('@')[0].split('abfss://')[1]
    file_path = abfss_path.split('@')[1].split('/')[1:]
    https_parquet_folder_path = posixpath.join('https://', storage_account_endpoint,  container, '/'.join(file_path))

    return https_parquet_folder_path