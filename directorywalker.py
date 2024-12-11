import os
import re
import logging
from pathlib import Path
from typing import List, Pattern, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

class DirectoryWalker:
    """
    A multithreaded directory traversal utility for finding files using regex patterns.

    This class allows for efficient file searching across directory structures by:
    - Applying regex patterns to match specific folders
    - Using regex to identify target files
    - Filtering files by extension
    - Leveraging multithreading to speed up file discovery
    - Optionally logging progress and results

    Attributes:
        base_path (Path): Root directory to start the search from
        folder_patterns (List[Pattern]): Compiled regex patterns for matching folders
        file_pattern (Pattern): Compiled regex pattern for matching files
        extension_pattern (Optional[Pattern]): Compiled regex pattern for file extensions
        threads (int): Number of threads to use for parallel processing
        logging_enabled (bool): Flag to enable/disable logging
        logger (logging.Logger): Logger instance for tracking progress and errors
    """

    def __init__(self, base_path: str, 
                 folder_patterns: List[str], 
                 file_pattern: str, 
                 extension_pattern: Optional[str] = r"\.txt$",
                 threads: int = None, 
                 logging_enabled: bool = True):
        """
        Initialize the DirectoryWalker with search parameters.

        Args:
            base_path (str): Root directory path to start searching from
            folder_patterns (List[str]): List of regex patterns to match folders
            file_pattern (str): Regex pattern to match target files
            extension_pattern (Optional[str]): Regex pattern for file extensions. 
                Defaults to ".txt" files. Set to None to skip extension filtering.
            threads (int, optional): Number of threads to use. Defaults to (CPU count - 2)
            logging_enabled (bool, optional): Enable/disable logging. Defaults to True
        """
        # Convert base path to Path object for efficient path manipulation
        self.base_path = Path(base_path)
        
        # Compile regex patterns for efficient matching
        self.folder_patterns = [re.compile(pattern) for pattern in folder_patterns]
        self.file_pattern = re.compile(file_pattern)
        
        # Compile extension pattern if provided
        self.extension_pattern = re.compile(extension_pattern) if extension_pattern is not None else None
        
        # Determine number of threads, defaulting to available CPUs minus 2
        self.threads = threads or max(os.cpu_count() - 2, 1)
        
        # Configure logging
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
        """
        Log a message if logging is enabled.

        Args:
            message (str): Message to log
        """
        if self.logging_enabled and self.logger:
            self.logger.info(message)

    def should_traverse(self, folder: Path) -> bool:
        """
        Determine if a folder should be traversed based on regex patterns.

        Args:
            folder (Path): Folder path to check

        Returns:
            bool: True if folder matches any of the defined patterns, False otherwise
        """
        return any(pattern.search(folder.name) for pattern in self.folder_patterns)

    def process_folder(self, folder: Path) -> List[str]:
        """
        Find files in a single folder that match the file and extension patterns.

        Args:
            folder (Path): Folder to search for matching files

        Returns:
            List[str]: List of full paths to matching files
        """
        matched_files = []
        for file in folder.iterdir():
            # Check if item is a file
            if file.is_file():
                # Check if file matches file pattern
                file_match = self.file_pattern.search(file.name)
                
                # Check extension pattern if specified
                extension_match = (self.extension_pattern is None or 
                                   self.extension_pattern.search(file.suffix))
                
                # Add file if both conditions are met
                if file_match and extension_match:
                    matched_files.append(str(file.resolve()))
        
        return matched_files

    def find_matching_files(self) -> List[str]:
        """
        Traverse directories and find files matching the file and extension regex patterns.

        Uses multithreading to improve search performance across large directory structures.

        Returns:
            List[str]: List of full paths to all matching files
        """
        matched_files = []
        
        # Count total folders to be processed for progress tracking
        total_folders = sum(1 for folder in self.base_path.rglob('*') 
                            if folder.is_dir() and self.should_traverse(folder))
        processed_folders = 0
        
        # Use ThreadPoolExecutor for parallel folder processing
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            # Submit folder processing tasks
            future_to_folder = {
                executor.submit(self.process_folder, folder): folder
                for folder in self.base_path.rglob('*') 
                if folder.is_dir() and self.should_traverse(folder)
            }
            
            # Process completed futures
            for future in as_completed(future_to_folder):
                processed_folders += 1
                matched_files.extend(future.result())
                
                # Log progress periodically (every 10%)
                if processed_folders % max(1, total_folders // 10) == 0:
                    self.log(f"Progress: {processed_folders}/{total_folders} folders processed.")
        
        # Final completion log
        self.log("File matching completed.")
        return matched_files

    def write_to_file(self, output_file: str, matched_files: List[str]) -> None:
        """
        Write the list of matched file paths to an output text file.

        Args:
            output_file (str): Path to the output file
            matched_files (List[str]): List of file paths to write

        Raises:
            IOError: If there's an error writing to the file
        """
        try:
            with open(output_file, 'w') as f:
                f.writelines(f"{file_path}\n" for file_path in matched_files)
            self.log(f"Results written to {output_file}")
        except IOError as e:
            self.log(f"Error writing to file: {e}")

# Example usage demonstrating how to use the DirectoryWalker
if __name__ == "__main__":
    # Configure search parameters
    base_path = "/path/to/network/directory"
    folder_patterns = [r"regex_for_folder1", r"regex_for_folder2"]
    file_pattern = r".*"  # Match all files
    output_file = "matched_files.txt"
    
    # Example usages with different extension filters
    # 1. Default .txt files
    walker_txt = DirectoryWalker(base_path, folder_patterns, file_pattern)
    
    # 2. Specific file extensions
    walker_py = DirectoryWalker(base_path, folder_patterns, file_pattern, r"\.py$")
    
    # 3. No extension filter
    walker_no_filter = DirectoryWalker(base_path, folder_patterns, file_pattern, None)
    
    # Run and write results
    matched_files_txt = walker_txt.find_matching_files()
    walker_txt.write_to_file(output_file, matched_files_txt)
