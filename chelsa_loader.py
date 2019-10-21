import os
import requests
import itertools
import re
from tqdm import tqdm

# Where from the data is grabbed
BASE_URL  = "https://www.wsl.ch/lud/chelsa/data/"

# Template where to save data
OUTPUT_FOLDER_TEMPLATE = r'/home/dmitry/bin/chelsa/{folder_name}/{model}/{year}/{emission}/'

# buffer size in bytes
BUFFER_SIZE = 1024 * 1024

SCHEME = {
    "CURRENT_mean_temp": {
                            'file_template' : r'CHELSA_temp10_{month}_1979-2013_V1.2_land.tif',
                            'month': ['{:02d}'.format(_) for _ in range(1, 13)],
                            'intermediate_url': 'climatologies/temp/integer/temp/',
                          },
    "CURRENT_min_temp": {
                            'file_template' : r'CHELSA_tmin10_{month}_1979-2013_V1.2_land.tif',
                            'month': ['{:02d}'.format(_) for _ in range(1, 13)],
                            'intermediate_url': 'climatologies/temp/integer/tmin',
                        },
    "CURRENT_max_temp": {
                            'file_template' : r'CHELSA_tmax10_{month}_1979-2013_V1.2_land.tif',
                            'month': ['{:02d}'.format(_) for _ in range(1, 13)],
                            'intermediate_url': 'climatologies/temp/integer/tmax/',
                        },

    "CURRENT_prec":     {
                            'file_template' : r'CHELSA_prec_{month}_V1.2_land.tif',
                            'month': ['{:02d}'.format(_) for _ in range(1, 13)],
                            'intermediate_url': 'climatologies/prec/',

                         },

    # LGM loader

    "LGM_prec": {
                    'file_template' : r'CHELSA_PMIP_{model}_prec_{month}_1.tif',
                    'month': ['{}'.format(_) for _ in range(1, 13)],
                    'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                    'intermediate_url': 'pmip3/prec/',
                 },

    "LGM_min_temp": {
                  'file_template' : r'CHELSA_PMIP_{model}_tmin_{month}_1.tif',
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'pmip3/tmin/',
                 },

    "LGM_max_temp": {
                  'file_template' : r'CHELSA_PMIP_{model}_tmax_{month}_1.tif',
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'pmip3/tmax/',
                 },

    "LGM_mean_temp": {
                  'file_template' : r'CHELSA_PMIP_{model}_tmean_{month}_1.tif',
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'pmip3/tmean/',
                 },

   # Future

    "FUTURE_prec": {
                  'file_template' : r'CHELSA_pr_mon_{model}_{emission}_r1i1p1_g025.nc_{month}_{year}.tif',
                  'year': ['2041-2060', '2061-2080'],
                  'emission': ['rcp26', 'rcp45', 'rcp60', 'rcp85'],
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'cmip5/{year}/prec/',
                 },


    "FUTURE_tmax": {
                  'file_template' : r'CHELSA_tasmax_mon_{model}_{emission}_r1i1p1_g025.nc_{month}_{year}_V1.2.tif',
                  'year': ['2041-2060', '2061-2080'],
                  'emission': ['rcp26', 'rcp45', 'rcp60', 'rcp85'],
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'cmip5/{year}/tmax/',
                 },


    "FUTURE_tmin": {
                  'file_template' : r'CHELSA_tasmin_mon_{model}_{emission}_r1i1p1_g025.nc_{month}_{year}_V1.2.tif',
                  'year': ['2041-2060', '2061-2080'],
                  'emission': ['rcp26', 'rcp45', 'rcp60', 'rcp85'],
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'cmip5/{year}/tmin/',
                 },

    "FUTURE_tmean": {
                  'file_template' : r'CHELSA_tas_mon_{model}_{emission}_r1i1p1_g025.nc_{month}_{year}_V1.2.tif',
                  'year': ['2041-2060', '2061-2080'],
                  'emission': ['rcp26', 'rcp45', 'rcp60', 'rcp85'],
                  'month': ['{}'.format(_) for _ in range(1, 13)],
                  'model': ['CCSM4', 'MRI-CGCM3','MIROC-ESM'],
                  'intermediate_url': 'cmip5/{year}/temp/',
                 },
}



def cartesian_helper(dct):
    """ Cartesian product applyied to  a dictionary """

    keys = dct.keys()
    vals = dct.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, instance))


def get_output_folder(**kwargs):
    """ Form output path using template  """

    template = OUTPUT_FOLDER_TEMPLATE
    to_render = dict()
    for kw in re.findall(r'\{([a-zA-Z_]+)\}', template):
        if kwargs.get(kw, None) is None:
            template = template.replace('/{%s}/' % kw, '/')
        else:
            to_render.update({kw: kwargs[kw]})
    return template.format(**to_render)


def download(url, output_path, dry_run=True):
    """ Download and save files according to above configurations """
    filename = url.split('/')[-1]
    response = requests.get(url, stream=True)
    output_file_path = os.path.join(output_path, filename)
    if os.path.exists(output_file_path):
        file_size = os.stat(output_file_path).st_size
    else:
        file_size = 0

    remote_file_size = response.headers.get('Content-length', 0)
    try:
        remote_file_size = int(remote_file_size)
    except ValueError:
        remote_file_size = 0

    if (abs(remote_file_size - file_size) > 0.2 * file_size) and file_size != 0:
        print('File sizes differ: ', remote_file_size, file_size)
        try:
            print("Removing file: ", output_file_path)
            os.remove(output_file_path)
        except (OSError, IOError):
            pass
    elif file_size == 0:
        print("The file wasn't downloaded: ", url)
    else:
        print("Files don't  differ: ", remote_file_size, file_size)
        print("The file is already downloaded: ", output_file_path)
        return

    if not dry_run:
        if remote_file_size:
            os.makedirs(output_path, exist_ok=True)
            loaded = 0
            print("Downloading started: ", url)
            with tqdm(total=remote_file_size) as pbar:
                with open(output_file_path, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=BUFFER_SIZE):
                        if chunk:
                            handle.write(chunk)
                        loaded += BUFFER_SIZE
                        pbar.update(loaded)
            print("The file was downloaded: ", output_file_path)
    else:
        print("Fake loading file: %s, size=%s" % (url, remote_file_size))


def main():
    total = 0
    for name, item in SCHEME.items():
        to_permute = dict()
        for var in item:
            if isinstance(item[var], list):
                to_permute.update({var: item[var]})

        for vars in cartesian_helper(to_permute):
            output_folder = get_output_folder(folder_name=name, **vars)
            url_full = os.path.join(BASE_URL,
                                    item['intermediate_url'].format(**vars),
                                    item['file_template'].format(**vars),
                                    )
            print("Starting download: ", url_full)
            download(url_full, output_folder, dry_run=False)
            total += 1
    print("Total number of files: ", total)


if __name__ == "__main__":
    main()
