from pathlib import Path
import re
from typing import List, Pattern
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging


class DirectoryWalker:
    """
    Class to traverse directories based on folder and file regex patterns,
    utilizing multithreading and logging for progress updates.
    """
    
    def __init__(self, base_path: str, folder_patterns: List[str], file_pattern: str, 
                 threads: int = None, logging_enabled: bool = True):
        self.base_path = Path(base_path)
        self.folder_patterns = [re.compile(pattern) for pattern in folder_patterns]
        self.file_pattern = re.compile(file_pattern)
        self.threads = threads or max(os.cpu_count() - 2, 1)  # Default: max processors - 2
        self.logging_enabled = logging_enabled

        if self.logging_enabled:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            self.logger = logging.getLogger("DirectoryWalker")
        else:
            self.logger = None

    def log(self, message: str) -> None:
        """Log a message if logging is enabled."""
        if self.logging_enabled and self.logger:
            self.logger.info(message)

    def should_traverse(self, folder: Path) -> bool:
        """Check if a folder matches any of the folder regex patterns."""
        return any(pattern.search(folder.name) for pattern in self.folder_patterns)

    def process_folder(self, folder: Path) -> List[str]:
        """Find matching files in a single folder."""
        matched_files = []
        for file in folder.iterdir():
            if file.is_file() and self.file_pattern.search(file.name):
                matched_files.append(str(file.resolve()))
        return matched_files

    def find_matching_files(self) -> List[str]:
        """Walk through directories and find files matching the file regex using multithreading."""
        matched_files = []
        total_folders = sum(1 for folder in self.base_path.rglob('*') if folder.is_dir() and self.should_traverse(folder))
        processed_folders = 0

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            future_to_folder = {
                executor.submit(self.process_folder, folder): folder
                for folder in self.base_path.rglob('*') if folder.is_dir() and self.should_traverse(folder)
            }

            for future in as_completed(future_to_folder):
                processed_folders += 1
                matched_files.extend(future.result())
                
                # Log progress at regular intervals
                if processed_folders % max(1, total_folders // 10) == 0:  # Log every 10% of progress
                    self.log(f"Progress: {processed_folders}/{total_folders} folders processed.")
        
        # Final log
        self.log("File matching completed.")
        return matched_files

    def write_to_file(self, output_file: str, matched_files: List[str]) -> None:
        """Write full paths of matched files to a text file."""
        with open(output_file, 'w') as f:
            f.writelines(f"{file_path}\n" for file_path in matched_files)
        self.log(f"Results written to {output_file}")


if __name__ == "__main__":
    # Example usage
    base_path = "/path/to/network/directory"
    folder_patterns = [r"regex_for_folder1", r"regex_for_folder2"]
    file_pattern = r"regex_for_files"
    output_file = "matched_files.txt"

    walker = DirectoryWalker(base_path, folder_patterns, file_pattern)
    matched_files = walker.find_matching_files()
    walker.write_to_file(output_file, matched_files)