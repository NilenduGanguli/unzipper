import zipfile
import os
from pathlib import Path

def unzip_nested_zip(zip_path, extract_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        extracted_files = zip_ref.namelist()
    
    # Loop through extracted files to find nested zip files
    for file_name in extracted_files:
        file_path = extract_dir / file_name
        if zipfile.is_zipfile(file_path):
            nested_dir = file_path.with_suffix('')
            nested_dir.mkdir(exist_ok=True)
            unzip_nested_zip(file_path, nested_dir)
            file_path.unlink()  # Remove the nested zip file after extracting

def build_tree(directory, prefix=""):
    contents = list(directory.iterdir())
    pointers = ["├── "] * (len(contents) - 1) + ["└── "]
    
    for pointer, path in zip(pointers, contents):
        print(prefix + pointer + path.name)
        if path.is_dir():
            extension = "│   " if pointer == "├── " else "    "
            build_tree(path, prefix + extension)

def main(zip_path):
    extract_dir = Path("extracted_files")
    extract_dir.mkdir(exist_ok=True)

    # Unzip all nested zips
    unzip_nested_zip(Path(zip_path), extract_dir)

    # Display tree structure
    print(f"{extract_dir.name}/")
    build_tree(extract_dir)

if __name__ == "__main__":
    zip_file_path = input("Enter the path of the zip file: ")
    main(zip_file_path)
