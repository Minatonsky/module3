from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import sys
import files_parser as parser
from normalize import normalize


def handle_media(filename: Path, target_folder: Path):
    target_folder.mkdir(exist_ok=True, parents=True)
    filename.replace(target_folder / normalize(filename.name))


def handle_other(filename: Path, target_folder: Path):
    target_folder.mkdir(exist_ok=True, parents=True)
    filename.replace(target_folder / normalize(filename.name))


def handle_archive(filename: Path, target_folder: Path):
    target_folder.mkdir(exist_ok=True, parents=True)
    folder_for_file = target_folder / normalize(filename.name.replace(filename.suffix, ''))
    folder_for_file.mkdir(exist_ok=True, parents=True)
    try:
        shutil.unpack_archive(str(filename.resolve()), str(folder_for_file.resolve()))
    except shutil.ReadError:
        folder_for_file.rmdir()
        return None
    filename.unlink()


def handle_folder(folder: Path):
    try:
        folder.rmdir()
    except OSError:
        print(f'Failed to delete folder {folder.resolve()}')


def main(folder: Path):
    parser.scan(folder)

    with ThreadPoolExecutor() as executor:
        for file in parser.IMAGES:
            executor.submit(handle_media, file, folder / 'images')

        for file in parser.VIDEO:
            executor.submit(handle_media, file, folder / 'video')

        for file in parser.AUDIO:
            executor.submit(handle_media, file, folder / 'audio')

        for file in parser.DOCUMENTS:
            executor.submit(handle_media, file, folder / 'documents')

        for file in parser.OTHER:
            executor.submit(handle_other, file, folder / 'OTHER')

        for file in parser.ARCHIVES:
            executor.submit(handle_archive, file, folder / 'archives')

        for folder in parser.FOLDERS[::-1]:
            handle_folder(folder)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        folder_for_scan = Path(sys.argv[1])
        print(f'Start in folder {folder_for_scan.resolve()}')
        main(folder_for_scan)
