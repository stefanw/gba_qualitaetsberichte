import functools

import pandas as pd


def replace_and_save_nan(hygiene_df, val_cols, replacement=0, suffix='_original'):
    for c in val_cols:
        hygiene_df[c + suffix] = hygiene_df[c]
        hygiene_df[c] = hygiene_df[c].fillna(replacement)
    return hygiene_df


def choose_non_null(row, key=None):
    try:
        v = row['%s_standort' % key]
        if pd.notnull(v):
            return v
    except KeyError:
        pass
    return row['%s_kh' % key]


def fix_standort(hygiene_df):
    for k in ('so', 'name', 'plz', 'ort', 'strasse', 'hausnr'):
        hygiene_df[k] = hygiene_df.apply(functools.partial(choose_non_null, key=k), 1)
        hygiene_df = hygiene_df.drop(['%s_kh' % k, '%s_standort' % k], 1)

    hygiene_df['year'] = hygiene_df['path_year']
    hygiene_df['ik-so'] = hygiene_df.apply(lambda x: '%s-%s' % (str(x['path_ik']), str(x['path_so'])), 1)
    hygiene_df['hausnr'] = hygiene_df['hausnr'].apply(lambda x: x if not isinstance(x, float) else int(x))
    hygiene_df['address'] = hygiene_df.apply(lambda x: '%s %s' % (x['strasse'], x['hausnr']), 1)
    hygiene_df['plz'] = hygiene_df['plz'].apply(lambda x: x if pd.isnull(x) else str(int(x)).zfill(5))
    hygiene_df['ik'] = hygiene_df['ik'].apply(lambda x: x if pd.isnull(x) else int(x))
    hygiene_df = hygiene_df.reset_index()
    return hygiene_df


def get_ik_bl(x):
    return str(x)[2:4]


def assign_bundesland(hygiene_df):
    bl_mapping = dict([
        ('01', 'Schleswig-Holstein',),
        ('02', 'Hamburg',),
        ('03', 'Niedersachsen',),
        ('04', 'Bremen',),
        ('05', 'Nordrhein-Westfalen',),
        ('06', 'Hessen',),
        ('07', 'Rheinland-Pfalz',),
        ('08', 'Baden-Württemberg',),
        ('09', 'Bayern',),
        ('10', 'Saarland',),
        ('11', 'Berlin',),
        ('12', 'Brandenburg',),
        ('13', 'Mecklenburg-Vorpommern',),
        ('14', 'Sachsen',),
        ('15', 'Sachsen-Anhalt',),
        ('16', 'Thüringen',)
    ])
    hygiene_df['ik_bl'] = hygiene_df['ik'].apply(get_ik_bl)
    hygiene_df['bundesland'] = hygiene_df['ik_bl'].apply(lambda x: bl_mapping.get(x, None))
    return hygiene_df
