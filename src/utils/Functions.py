from pathlib import Path
from glob import iglob

def list_data(path):
    '''List all unprocessed data files in the path

    Parameters
    ----------
    path : str
        Path to the unprocessed data
    Returns
    -------
    list
        List of unprocessed data files
    '''
    data_path = Path(path)
    files = [x for x in iglob(str(data_path / "*"))] if data_path.exists() else []
    if len(files) == 0:
        raise FileNotFoundError(f"No files found in {path}")
    files.sort()
    data = [fpath for fpath in files]
    data.sort()
    return data