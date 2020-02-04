# GlacierProject

## Usage Steps
1. Open `main.py`
2. Edit the `run_pipeline` method parameters inside the `main` method at the end with:
    a. Either a list of GLIMS IDs or a textfile with GLIMS IDs (by specifying a delimiter)
    b. The data directory
    c. Currently doesn't support ee method parameters yet. If you want to edit, edit the parameters in `single_glacier`
3. Preparations before running:
    a. Have the `client_secrets.json` Google Drive API file inside the working directory
    b. Edit root folder name at the top of `drive.py` if desired (currently set to 'glaciers') 
4. Run
    a. Change directory in terminal to the directory with source code (`GlacierProject`)
    b. Run `python3 main.py` for unix, `python main.py` for Windows