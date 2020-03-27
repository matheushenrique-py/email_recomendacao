import zipfile
from io import BytesIO

def build_zipfile(files_dic):
    memory_file = BytesIO()

    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED, False) as zf:
        for key, value in files_dic.items():
            zf.writestr(key, value.getvalue())

    memory_file.seek(0)
    return memory_file