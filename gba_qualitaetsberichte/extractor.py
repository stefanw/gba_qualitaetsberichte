import glob
import gzip
import os

import pandas as pd
import pyprind
from lxml import etree


QB_QUERY = {
    'ik': './Krankenhaus/Kontaktdaten/IK',

    'so_kh': './Krankenhaus/Kontaktdaten/Standortnummer',
    'name_kh': './Krankenhaus/Kontaktdaten/Name',
    'plz_kh': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_kh': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Ort',
    'strasse_kh': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_kh': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Hausnummer',

    'so_standort': './Standort_dieses_Berichts/Kontaktdaten/Standortnummer',
    'name_standort': './Standort_dieses_Berichts/Kontaktdaten/Name',
    'plz_standort': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_standort': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Ort',
    'strasse_standort': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_standort': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Hausnummer',

    'traeger': './Krankenhaustraeger/Name',
    'traeger_art': './Krankenhaustraeger/Krankenhaustraeger_Art/Art'
}


def get_root(path):
    if path.endswith('.gz'):
        with gzip.open(path) as f:
            return etree.parse(f)
    with open(path) as f:
        return etree.parse(f)


def get_paths(paths, exclude=None, include=None):
    for path in paths:
        if include is not None and not any(x in path for x in include):
            continue
        if exclude is not None and any(x in path for x in exclude):
            continue
        yield path


def apply_patches(df, path):
    patches = pd.read_csv(path)
    for _, patch in patches.iterrows():
        val = patch['value']
        if patch['type'] == 'int':
            val = int(val)
        df.loc[(df['path'] == patch['path']), patch['field']] = val
    return df


class PathIterator(object):
    def __init__(self, data_paths=None, exclude=None, include=None,
                 base_query=None, file_pattern='*.xml.gz'):
        self.paths = self.construct_paths(data_paths, exclude, include,
                                          file_pattern)
        self.base_query = base_query
        if self.base_query is None:
            self.base_query = {}

    def construct_paths(self, data_paths, exclude, include, file_pattern):
        paths = []
        for data_path in data_paths:
            names = glob.glob(os.path.join(data_path, file_pattern))
            paths.extend(list(get_paths(names, exclude=exclude, include=include)))
        return paths

    def get_val(self, node, xpath):
        matches = node.xpath(xpath)
        if not matches:
            return None
        return self.convert_match(matches[0])

    def convert_match(self, match):
        if match.text is None:
            return True
        val = match.text.strip()
        try:
            val = float(val.replace(',', '.'))
        except ValueError:
            pass
        return val

    def run_query(self, query):
        updater = pyprind.ProgPercent(len(self.paths))
        for path in self.paths:
            updater.update()
            yield from self.run_query_for_path(path, query)

    def run_query_for_path(self, path, query):
        root = get_root(path)
        new_query = dict(query)
        new_query.update(self.base_query)
        base_data = self.get_path_info(path)
        return self.run_sub_query(new_query, root, base_data=base_data)

    def run_sub_query(self, query, node, base_data=None):
        data = {}
        if base_data is not None:
            data.update(base_data)

        sub_xpath = None
        for key, val in query.items():
            if isinstance(val, dict):
                if sub_xpath:
                    raise ValueError('There must only be one deeper xpath per level (%s)' % sub_xpath)
                sub_xpath = key
            else:
                data[key] = self.get_val(node, val)

        if sub_xpath is not None:
            new_query = query[sub_xpath]
            for sub_node in node.xpath(sub_xpath):
                for sub_data in self.run_sub_query(new_query, node=sub_node):
                    new_data = dict(data)
                    new_data.update(sub_data)
                    yield new_data
        else:
            yield data

    def get_path_info(self, path):
        path_parts = path.split('/')[-1].split('-')
        return {
            'path': path,
            'path_ik': path_parts[0],
            'path_so': path_parts[1],
            'path_year': int(path_parts[2]),
        }


class QualityReports(object):
    def __init__(self, path='data/', years=[2014], base_query=QB_QUERY):
        self.path = path
        self.base_query = base_query
        self.years = years

    def query(self, query):
        data_paths = [self.get_year_path(year) for year in self.years]
        pit = PathIterator(data_paths=data_paths, exclude=('-99-',),
                           base_query=self.base_query)
        return pd.DataFrame(pit.run_query(query))

    def get_year_path(self, year):
        return os.path.join(self.path, 'base_%s' % year)


def apply_func(func, include=None, exclude=None, data_path='data/base_2015'):
    for path in glob.glob(data_path + '/*.xml.gz'):
        if include is not None and not any(x in path for x in include):
            continue
        if exclude is not None and any(x in path for x in exclude):
            continue
        root = get_root(path)
        yield func(root)
