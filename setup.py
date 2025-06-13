import os
import sys
import shutil
import subprocess
import platform

import os
import sys
import shutil
import subprocess
import platform
import time
import ctypes

def is_admin():
    """Check if the script is running with administrator privileges."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def setup_environment():
    """Set up the development environment by creating a new virtual environment
    and installing the required packages."""
    
    print("Setting up the development environment...")
    
    # Check for admin rights on Windows
    if platform.system() == "Windows" and not is_admin():
        print("Warning: Script is not running with administrator privileges.")
        print("Some operations might fail due to permission issues.")
        
    # Determine the correct Python executable
    python_cmd = "python" if platform.system() == "Windows" else "python3"
    
    # Check if venv exists and remove it if it does
    if os.path.exists("venv"):
        print("Removing existing virtual environment...")
        try:
            # Wait a moment to ensure no processes are using the directory
            time.sleep(1)
            
            # First, try to make files writable
            if platform.system() == "Windows":
                try:
                    subprocess.run(f'attrib -R "venv\\*.*" /S /D', shell=True, check=False)
                except:
                    pass
                    
            # Then remove directory
            shutil.rmtree("venv", ignore_errors=True)
            
            # Double check if it's gone
            if os.path.exists("venv"):
                print("Failed to remove venv directory completely.")
                print("Waiting 5 seconds to retry...")
                time.sleep(5)
                shutil.rmtree("venv", ignore_errors=True)
        except Exception as e:
            print(f"Warning: Could not remove venv directory completely: {e}")
            print("Please close any running processes and remove the directory manually.")
            return False
    
    # Create a new virtual environment
    print("Creating new virtual environment...")
    try:
        # Try with explicit temporary directory path
        temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_venv")
        subprocess.check_call([python_cmd, "-m", "venv", "--clear", "venv"])
    except subprocess.CalledProcessError as e:
        print(f"Failed to create virtual environment: {e}")
        print("This could be due to permission issues.")
        
        if platform.system() == "Windows":
            print("\nSuggestions:")
            print("1. Run the script as administrator")
            print("2. Check if antivirus is blocking the creation of executable files")
            print("3. Try running the script from a directory where you have full permissions")
            print("4. Close any applications that might be accessing Python files")
        return False
    except PermissionError:
        print("Permission denied when creating the virtual environment.")
        print("Try running the script as administrator.")
        return False
    
    # Determine the path to the virtual environment's Python executable
    if platform.system() == "Windows":
        venv_python = os.path.join("venv", "Scripts", "python.exe")
        venv_pip = os.path.join("venv", "Scripts", "pip.exe")
        activate_script = os.path.join("venv", "Scripts", "activate")
    else:
        venv_python = os.path.join("venv", "bin", "python")
        venv_pip = os.path.join("venv", "bin", "pip")
        activate_script = os.path.join("venv", "bin", "activate")
        
    # Check if the virtual environment's Python executable exists
    if not os.path.exists(venv_python):
        print(f"Error: Virtual environment created but {venv_python} not found.")
        print("The virtual environment might be corrupted.")
        return False

    # Rest of the function remains the same...
    """Set up the development environment by creating a new virtual environment
    and installing the required packages."""
    
    print("Setting up the development environment...")
    
    # Determine the correct Python executable
    python_cmd = "python" if platform.system() == "Windows" else "python3"
    
    # Check if venv exists and remove it if it does
    if os.path.exists("venv"):
        print("Removing existing virtual environment...")
        try:
            # On Windows, sometimes files are locked, so we need to handle errors
            shutil.rmtree("venv", ignore_errors=True)
        except Exception as e:
            print(f"Warning: Could not remove venv directory completely: {e}")
            print("Please close any running processes and remove the directory manually.")
            return False
    
    # Create a new virtual environment
    print("Creating new virtual environment...")
    try:
        subprocess.check_call([python_cmd, "-m", "venv", "venv"])
    except subprocess.CalledProcessError:
        print("Failed to create virtual environment.")
        return False
    
    # Determine the path to the virtual environment's Python executable
    if platform.system() == "Windows":
        venv_python = os.path.join("venv", "Scripts", "python.exe")
        venv_pip = os.path.join("venv", "Scripts", "pip.exe")
        activate_script = os.path.join("venv", "Scripts", "activate")
    else:
        venv_python = os.path.join("venv", "bin", "python")
        venv_pip = os.path.join("venv", "bin", "pip")
        activate_script = os.path.join("venv", "bin", "activate")
    
    # Upgrade pip to the latest version
    print("Upgrading pip...")
    try:
        subprocess.check_call([venv_python, "-m", "pip", "install", "--upgrade", "pip"])
    except subprocess.CalledProcessError:
        print("Failed to upgrade pip.")
        return False
    
    # Install pymongo
    print("Installing pymongo...")
    try:
        subprocess.check_call([venv_python, "-m", "pip", "install", "pymongo"])
    except subprocess.CalledProcessError:
        print("Failed to install pymongo.")
        return False
    
    # Install other requirements if requirements.txt exists
    if os.path.exists("requirements.txt"):
        print("Installing requirements...")
        try:
            subprocess.check_call([venv_python, "-m", "pip", "install", "-r", "requirements.txt"])
        except subprocess.CalledProcessError:
            print("Failed to install some requirements.")
            return False
    
    print("\nSetup completed successfully!")
    print(f"\nTo activate the virtual environment:")
    if platform.system() == "Windows":
        print(f"    {activate_script}")
    else:
        print(f"    source {activate_script}")
    
    return True

if __name__ == "__main__":
    setup_environment()
