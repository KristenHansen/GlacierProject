# GlacierProject

## Usage Steps
1. Open `main.py`
2. Edit the `run_pipeline` method parameters inside the `main` method at the end with:
    1. Either a list of GLIMS IDs or a textfile with GLIMS IDs (by specifying a delimiter)
    2. The data directory
    3. Currently doesn't support ee method parameters yet. If you want to edit, edit the parameters in `single_glacier`
3. Preparations before running:
    1. Have the `client_secrets.json` Google Drive API file inside the working directory
    2. Edit root folder name at the top of `drive.py` if desired (currently set to 'glaciers') 
4. Run
    1. Change directory in terminal to the directory with source code (`GlacierProject`)
    2. Run `python3 main.py` for unix, `python main.py` for Windows