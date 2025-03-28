"""
Version control functions for JSONLDB using Git.
"""

import os
from typing import Dict
import git
from jsonldb.jsonlfile import lint_jsonl
from datetime import datetime
import warnings


def is_versioned(folder_path: str) -> bool:
    """Check if a folder is versioned.
    
    Args:
        folder_path (str): Path to the folder to check
    """
    return os.path.exists(os.path.join(folder_path, ".git"))


def init_folder(folder_path: str) -> None:
    """Initialize a folder as a git repository.
    
    Args:
        folder_path (str): Path to the folder to initialize
        
    Raises:
        git.exc.GitCommandError: If git commands fail
    """
    try:
        # Check if folder exists
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        # Try to initialize git repo
        try:
            repo = git.Repo(folder_path)
            warnings.warn(f"Folder {folder_path} is already a git repository")
        except git.exc.InvalidGitRepositoryError:
            # Initialize new repository
            repo = git.Repo.init(folder_path)
            print(f"Initialized git repository in {folder_path}")
            
    except git.exc.GitCommandError as e:
        raise git.exc.GitCommandError(f"Failed to initialize git repository: {str(e)}")

def commit(folder_path: str, msg: str = "") -> None:
    """Commit changes in the repository.
    
    Args:
        folder_path (str): Path to the git repository
        msg (str, optional): Commit message. If empty, uses auto-generated message
        
    Raises:
        git.exc.GitCommandError: If git commands fail
    """
    try:
        repo = git.Repo(folder_path)
        
        # Generate commit message
        timestamp = datetime.now().strftime("%Y-%m-%d@%H-%M")
        if msg:
            commit_msg = f"Manual Commit: {timestamp} {msg}"
        else:
            commit_msg = f"Auto Commit: {timestamp}"
            
        # Add all changes
        repo.index.add("*")
        
        # Commit changes
        repo.index.commit(commit_msg)
        print(f"Committed changes with message: {commit_msg}")
        
    except git.exc.GitCommandError as e:
        raise git.exc.GitCommandError(f"Failed to commit changes: {str(e)}")

def list_version(folder_path: str) -> Dict[str, str]:
    """List all commits in the repository.
    
    Args:
        folder_path (str): Path to the git repository
        
    Returns:
        Dict[str, str]: Dictionary with commit hashes as keys and commit messages as values
        
    Raises:
        git.exc.GitCommandError: If git commands fail
    """
    try:
        repo = git.Repo(folder_path)
        
        # Get all commits
        commits = {}
        for commit in repo.iter_commits():
            commits[commit.hexsha[:7]] = commit.message.strip()
            
        return commits
        
    except git.exc.GitCommandError as e:
        raise git.exc.GitCommandError(f"Failed to list versions: {str(e)}")

def revert(folder_path: str, version_hash: str) -> None:
    """Revert the repository to a specific version.
    
    Args:
        folder_path (str): Path to the git repository
        version_hash (str): Hash of the commit to revert to
        
    Raises:
        git.exc.GitCommandError: If git commands fail
    """
    try:
        repo = git.Repo(folder_path)

        full_hash = repo.git.rev_parse(version_hash)
        
        # Check if commit exists
        try:
            commit = repo.commit(full_hash)
        except git.exc.BadName:
            raise ValueError(f"Commit {full_hash} not found")
            
        # Reset to the specified commit
        repo.git.reset(full_hash, hard=True)
        print(f"Reverted to commit {full_hash}")
        
    except git.exc.GitCommandError as e:
        raise git.exc.GitCommandError(f"Failed to revert to version {version_hash}: {str(e)}") 