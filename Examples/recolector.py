#
# Only for direct use without installing the library
import os
import sys
sys.path.insert(0, os.path.dirname(os.getcwd()))
#
import numpy as np
import pandas as pd
import datetime as dt
from collections import defaultdict as ddict
import MDBReader as MDBR
from MDBReader import get_collection_property_from_dataframe as MDBR_get_cp
from MDBReader import get_parent_collection_property_from_dataframe as MDBR_get_pcp

QUERY_get_data_0 = """
SELECT
    t_object.name AS Parent,
    t_collection.name AS Collection,
    t_object_1.name AS Child,
    t_property.name AS Property,
    t_period_0.datetime AS [Datetime],
    t_data_0.value AS [Value],
    t_collection.collection_id,
    t_key.phase_id
FROM
    (
        (
            (
                (
                    (
                        (t_data_0 INNER JOIN t_key ON t_data_0.key_id = t_key.key_id)
                        INNER JOIN
                        t_membership ON t_key.membership_id = t_membership.membership_id
                    )
                    INNER JOIN
                    t_period_0 ON t_data_0.period_id = t_period_0.interval_id
                )
                INNER JOIN
                t_property ON t_key.property_id = t_property.property_id
            )
            INNER JOIN
            t_collection ON t_membership.collection_id = t_collection.collection_id
        )
        INNER JOIN
        t_object ON t_membership.parent_object_id = t_object.object_id
    )
    INNER JOIN
    t_object AS t_object_1 ON t_membership.child_object_id = t_object_1.object_id
ORDER BY
    t_key.model_id,
    t_key.sample_id,
    t_property.property_id,
    t_membership.membership_id,
    t_data_0.period_id
;
"""


def process_years_weeks_from_mdb(BaseDir, Hidro, ResAmt, years=list(), weeks=list(), write_excel=False):
    """ Process for each hidrology, Year and Week. """
    dfs = ddict(list)
    CostReservaTot = list()
    CostGenTot = list()
    TotalCost = list()
    for NYeay in years:
        for NWeek in weeks:
            fpFile = BaseDir + os.sep
            fpFile += f'...Paste you path {NYear}-{NWeek} and so on... Solution.mdb'
            print("Reading:", NYeay, fpFile.split('\\')[-1])
            with MDBR.connect_db(fpFile) as conn:
                dfRes = MDBR.query_db_connection(conn, QUERY_get_data_0)
            #
            # Ordena los resultados según necesidad.
            dfTotGenCost = MDBR_get_cp(dfRes, colletion_name='Regions', property_name='Total Generation Cost')
            dfTotGenCost.rename(columns={'SMAY': 'GenCost USD'}, inplace=True)
            dfShortageCost = MDBR_get_cp(dfRes, colletion_name='Reserves', property_name='Shortage Cost')
            dfResUpAysen = MDBR_get_pcp(dfRes,
                                        colletion_name='Generators',
                                        property_name='Provision',
                                        parent_name='CPF+&CSF+')
            dfResUpCoyhaique = MDBR_get_pcp(dfRes,
                                            colletion_name='Generators',
                                            property_name='Provision',
                                            parent_name='CPF+&CSF+')
            dfResDownAysen = MDBR_get_pcp(dfRes,
                                          colletion_name='Generators',
                                          property_name='Provision',
                                          parent_name='CPF-&CSF-')
            dfResDownCoyhaique = MDBR_get_pcp(dfRes,
                                              colletion_name='Generators',
                                              property_name='Provision',
                                              parent_name='CPF-&CSF-')
            dfs['TotGenCost USD'].append(dfTotGenCost)
            dfs['Reserva falla USD'].append(dfShortageCost)
            dfs['ResUp Aysen MW'].append(dfResUpAysen)
            dfs['ResUp Coyhaique MW'].append(dfResUpCoyhaique)
            dfs['ResDown Aysen MW'].append(dfResDownAysen)
            dfs['ResDown Coyhaique MW'].append(dfResDownCoyhaique)
            dfs['TotGenCost'].append(dfTotGenCost)
            dfs['Despacho MW'].append(MDBR_get_cp(dfRes, colletion_name='Generators', property_name='Generation'))
            dfs['Reserva falla MW'].append(MDBR_get_cp(dfRes, colletion_name='Reserves', property_name='Shortage'))
            dfs['CMg USD_MW'].append(MDBR_get_cp(dfRes, colletion_name='Nodes', property_name='Price'))
            dfs['Flujo MW'].append(MDBR_get_cp(dfRes, colletion_name='Lines', property_name='Flow'))
            #
            CostReservaTot.append(dfShortageCost.sum().sum())
            CostGenTot.append(dfTotGenCost.sum().sum())
            TotalCost.append(CostReservaTot[-1] + CostGenTot[-1])

    if write_excel:
        # Consolida los Dataframe
        outDict = dict()
        for dfnom, dflist in dfs.items():
            df_aux = pd.concat(dflist, axis='index')
            outDict[dfnom] = df_aux
        # escribe archivo
        ResAmtText = f'{ResAmt:0.2f}'.replace('.', '_')
        MDBR.write_output_excel(f'Reserva{Hidro}+{ResAmtText}MW.xlsx', outDict)

    return TotalCost


if __name__ == '__main__':
    BaseDir = r'...Paste you path here...'

    #
    # timing
    tt = [dt.datetime.now()]
    #
    for Hidro in ['HIDSEC', 'HIDMED', 'HIDHUM']:
        TotalCosts = dict()
        for ResAmt in np.arange(0.7, 6, 0.25):
            years = [2022, 2023, 2024, 2025]
            weeks = [1, 2, 3, 4]
            #
            TotalCost = process_years_weeks_from_mdb(BaseDir, Hidro, ResAmt, years, weeks, write_excel=True)
            TotalCosts[ResAmt] = TotalCost
        #
        tblIndex = [f'{y}-S{w}' for y in years for w in weeks]
        dfCostTot = pd.DataFrame(TotalCosts, index=tblIndex).T
        dfCostTot.index.name = 'MW'
        dfCostTot.to_excel(f'Costo Total {Hidro}.xlsx')
        print(dfCostTot)
        #
        # timing
        tt.append(dt.datetime.now())
    print("Partial time:", [tt[i] - tt[i - 1] for i in range(1, len(tt))])
    print("Total time:", tt[-1] - tt[0])
    #
