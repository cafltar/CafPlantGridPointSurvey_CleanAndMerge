"""
Reads harvest data entry templates from 2017-2019 then:
  1. Adjust/removes values based on QA done by Ian Leslie
  2. Appends data from other files (NIR results)
  3. Calculates yield (field dry and standard moisture at 12.5%) and biomass on area basis
  4. Cleans headers and merges datasets
  5. Prints to csv file
"""

import pandas as pd
import geopandas as gpd
import glob
import os
import numpy as np
import time

def process_qc_manual_review(pathToQAFile, df, idColName):
    qa = pd.read_csv(pathToQAFile)

    for index, row in qa.iterrows():
        if(np.isnan(row["NewVal"])):
            df.loc[(df[idColName] == row["ID"]), row["Variable"]] = None
        else:
            df.loc[(df[idColName] == row["ID"]), row["Variable"]] = row["NewVal"]
    
    return df

def process_qc_bounds_check(df, idColName, lower, upper):
    df_out = df.copy()

    qc_applied = idColName + "_qcApplied"
    qc_result = idColName + "_qcResult"

    # TODO: Need to handle adding previous QC checks - use bin() / bit manip
    # 000010 means level 2 check, or check operating on single value
    df_out[qc_applied] = "000010"
    
    # result code defaults to 0 (passed), set to 2 (fail) if outside of bounds
    df_out[qc_result] = "000000"
    df_out.loc[((df_out[idColName] < lower) | (df_out[idColName] > upper)), qc_result] = "000010"

    return df_out


def mergeNir2019(df, dirPathToNirFiles):
    nirFiles = glob.glob(
        os.sep.join([dirPathToNirFiles, "NIR*.csv"]))

    df_nir = pd.DataFrame(columns = ["ID2", "ProtDM", "Moisture", "StarchDM", "WGlutDM"])

    for nirFile in nirFiles:
        nir = pd.read_csv(nirFile)
        nir_parse = (
            nir.query("Sample_ID.str.upper().str.contains('GP2019')")
            .assign(ID2 = 
                nir["Sample_ID"].str
                .upper().str
                .split("GP", expand = True)[0].str
                .replace("CE", "").str
                .replace("CW", ""))
            .astype({"ID2": int})
        )

        nir_clean = nir_parse[["ID2", "ProtDM", "Moisture", "StarchDM", "WGlutDM"]]
        df_nir = df_nir.append(nir_clean, ignore_index = True)
    
    df_merge = df.merge(df_nir, on = "ID2", how = "left")

    return df_merge

def clean2017(df, areaHarvested, georefPoints):
    # QA
    # Create column for crop presence, assume all crop present
    df["CropExists"] = 1

    # Overwrite any values in QA doc
    qaPath = os.sep.join(["input", "HY2017_QA_20200203.csv"])
    df_qa = process_qc_manual_review(qaPath, df, "Total Biomass Barcode ID")

    # TODO: Add Residue columns? Find C, N values for gain and residue, add other NIR columns
    # Note: In 2017, the grain was dried in oven before NIR analysis (that obtains Moisture %). In other years NIR is done before oven drying. This year, then, is an exception to calculations where we calculate standard weights at 12.5% moisture using oven dried mass, not field dried
    df_calc = (
        df_qa.assign(
            HarvestYear = 2017,
            BiomassDryPerArea = (df_qa["Dried Total Biomass mass + bag(g) + bags inside"] - df_qa["Average Dried total biomass bag + empty grain bag & empty residue bag inside mass (g)"])/areaHarvested,
            GrainYieldDryPerArea = (df_qa["Non-Oven dried grain mass (g)"] - df_qa["Average Non-Oven dried grain bag mass (g)"])/areaHarvested,
            GrainYield0 = 
                (df_qa["Oven dried grain mass (g)"] - df_qa["Average Non-Oven dried grain bag mass (g)"]) - 
                ((df_qa["Oven dried grain mass (g)"] - df_qa["Average Non-Oven dried grain bag mass (g)"]) * (df_qa["Moisture"] / 100.0)),
            Crop = df_qa["Total Biomass Barcode ID"].str.split("_", expand = True)[2],
            ID2 = df_qa["Total Biomass Barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            GrainMoisture = df_qa["Moisture"],
            GrainProtein = df_qa["Protein"],
            GrainStarch = df_qa["Starch"],
            GrainGluten = df_qa["WGlutDM"],
            GrainTestWeight = df_qa["Test Weight\n(Manual, small container if no # in large container column)"],
            Comments = df_qa["Notes and comments by Ian Leslie October 2019"],
            SampleID = df_qa["Total Biomass Barcode ID"]
        )
        .astype({"ID2": int})
    )

    df_calc = df_calc.assign(
        GrainYield125PerArea = (df_calc.GrainYield0 + (df_calc.GrainYield0 * 0.125)) / areaHarvested
    )

    df_clean = (
        df_calc[[
            "HarvestYear", 
            "ID2", 
            "SampleID", 
            "Crop", 
            "GrainYieldDryPerArea", 
            "BiomassDryPerArea", 
            "GrainYield125PerArea", 
            "GrainTestWeight",
            "GrainMoisture", 
            "GrainProtein",
            "GrainStarch",
            "GrainGluten",
            "CropExists", 
            "Comments"]]
        .merge(georefPoints, on = "ID2")
        .drop(["geometry"], axis = 1)
    )

    return df_clean

def clean2018(df, areaHarvested, georefPoints):
    # QA
    # Create column for crop presence, assume all crop present
    df["CropExists"] = 1
    # Overwrite any values in QA doc
    qaPath = os.sep.join(["input", "HY2018_QA_20200203.csv"])
    df_qa = process_qc_manual_review(qaPath, df, "total biomass bag barcode ID")

    # Note: No NIR data collected for garbs
    df_calc = (df_qa
        .assign(
            AreaOfInterestID2 = df_qa["total biomass bag barcode ID"].str.split("_", expand = True)[0],
            HarvestYear = 2018,
            ID2 = df_qa["total biomass bag barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            Crop = df_qa["total biomass bag barcode ID"].str.split("_", expand = True)[2],
            SampleID = df_qa["total biomass bag barcode ID"],
            BiomassDryPerArea = (df_qa["dried total biomass mass + bag + residue bag + grain bag (g)"] - df_qa["average dried empty total biomass bag +  grain bag + residue bag  (g)"])/areaHarvested,
            GrainYieldDryPerArea = (df_qa["non-oven dried grain mass + bag (g)"] - df_qa["average empty dried grain bag mass (g)"])/areaHarvested,
            Comments = df_qa["notes"].astype(str) + "| " + df_qa["Notes by Ian Leslie 10/22/2019"]
        )
        .query("(AreaOfInterestID2.str.contains('CW') | AreaOfInterestID2.str.contains('CE'))")
        .astype({"ID2": int})
    )

    df_clean = (
        df_calc[["HarvestYear", "ID2", "SampleID", "Crop", "GrainYieldDryPerArea", "BiomassDryPerArea", "CropExists", "Comments"]]
        .merge(georefPoints, on = "ID2")
        .drop(["geometry"], axis = 1)
    )

    return df_clean

def clean2019(df, areaHarvested, georefPoints):
    ### Data Preparation
    df_prep = df.copy()

    df_prep = (
        df_prep.assign(
            HarvestYear = 2019,
            ID2 = df["Total biomass bag barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            Crop = df["Total biomass bag barcode ID"].str.split("_", expand = True)[3],
            SampleID = df["Total biomass bag barcode ID"].str.upper(),
            ProjectID = df["Project ID"])
        .query("ProjectID == 'GP' & (SampleID.str.contains('CE') | SampleID.str.contains('CW'))")
        .astype({"ID2": int})
    )

    # Create column for crop presence, assume all crop present
    df_prep["CropExists"] = 1

    # Merge all NIR data with dataframe
    df_prep = mergeNir2019(df_prep, os.sep.join([os.getcwd(), "input", "HY2019_NIR"]))
    df_prep = (
        df_prep.assign(
            GrainMoisture = df_prep["Moisture"],
            GrainProtein = df_prep["ProtDM"],
            GrainStarch = df_prep["StarchDM"],
            GrainGluten = df_prep["WGlutDM"],
        )
    )

    # Merge lat/lon
    df_prep = df_prep.merge(georefPoints, on = "ID2")
    
    ### Resolve Manual Override

    # Overwrite any values in QA doc
    qaPath = os.sep.join(["input", "HY2019_QA_20200203.csv"])
    df_override = process_qc_manual_review(qaPath, df_prep, "Total biomass bag barcode ID")

    ### Calculation

    # Convert col types for math power
    #numeric_cols = ["Dried total biomass (g)", "Non-oven-dried grain (g)"]
    #df_merge[numeric_cols] = df_merge[numeric_cols].apply(pd.to_numeric, errors = "ignore")

    # TODO: Add test weight (convert from g to lb/bu using 0.0705), add NIR results, add C,N values
    df_calc = (
        df_override.assign(
            BiomassDryPerArea = df_override["Dried total biomass (g)"] / areaHarvested,
            GrainYieldDryPerArea = df_override["Non-oven-dried grain (g)"] / areaHarvested,
            GrainYield0 = 
                df_override["Non-oven-dried grain (g)"] - 
                (df_override["Non-oven-dried grain (g)"] * (df_override["Moisture"] / 100.0)),
            GrainTestWeight = df_override["Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  "] * 0.0705,
            Comments = df_override["Notes"].astype(str) + "| " + df_override["Notes made by Ian Leslie"]
        )
    )

    df_calc = df_calc.assign(
        GrainYield125PerArea = 
            (df_calc.GrainYield0 + (df_calc.GrainYield0 * 0.125)) / areaHarvested
    )

    ### Quality Control
    df_auto_qc = process_qc_bounds_check(df_calc, "GrainTestWeight", 0.0, 63.0)
    df_auto_qc = process_qc_bounds_check(df_auto_qc, "GrainMoisture", 7.0, 25.0)
    df_auto_qc = process_qc_bounds_check(df_auto_qc, "GrainProtein", 7.0, 22.0)
    df_auto_qc = process_qc_bounds_check(df_auto_qc, "GrainStarch", 52.0, 75.0)
    df_auto_qc = process_qc_bounds_check(df_auto_qc, "GrainGluten", 14.0, 45.0)

    ### Resolve Manual Review

    ###
    df_clean = (
        df_calc[[
            "HarvestYear", 
            "ID2", 
            "SampleID", 
            "Crop", 
            "GrainYieldDryPerArea", 
            "BiomassDryPerArea", 
            "GrainYield125PerArea",
            "GrainTestWeight", 
            "GrainMoisture",
            "GrainProtein",
            "GrainStarch",
            "GrainGluten", 
            "CropExists", 
            "Comments"]]
    )

    return df_clean

def main():
    # Constants
    AREA_HARVESTED = 2.4384

    # TODO: Filter for Project ID == "GP"
    #df_legacy = pd.read_csv(r"input\HY1999-2016_20200130_P3A1.csv")
    # 8 rows of header, last 2 rows are junk values
    df2017 = pd.read_excel(
        os.sep.join(["input", "LTAR_CAF_HY2017_CropBiomass-10-31-2017_IL_20191209.xlsx"]), 
        sheet_name="Sheet1", 
        skiprows=8, 
        nrows = 532,
        na_values=["N/A", ".", ""])
    #df2017qa = pd.read_csv(r"input\HY2017_QA.csv")
    df2018 = pd.read_excel(
        os.sep.join(["input", "LTARcafHarSamp2018HYBioGrainMasses10242018_IL_20191209.xlsx"]), 
        sheet_name="CAF Harvest Biomass Grain Data", 
        skiprows=7,
        na_values=["N/A", ".", ""])
    df2019 = pd.read_excel(
        os.sep.join(["input", "Harvest01_2019_GP-ART-Lime_INT__20191106_IL_20191209.xlsm"]), 
        sheet_name="Harvest01_2019", 
        skiprows=6,
        na_values=["N/A", ".", ""])

    cwgp = gpd.read_file(os.sep.join(["input", "cookwest_georeferencepoint_20190924.geojson"]))
    cegp = gpd.read_file(os.sep.join(["input", "cookeast_georeferencepoint_20190924.geojson"]))
    gp = pd.concat([cwgp, cegp])
    gp = gp.assign(Latitude = gp.geometry.y,Longitude = gp.geometry.x)

    # Prepare 2017
    df2017 = clean2017(df2017, AREA_HARVESTED, gp)
    df2018 = clean2018(df2018, AREA_HARVESTED, gp)
    df2019 = clean2019(df2019, AREA_HARVESTED, gp)

    # Concat 2017 - 2019 and assign geocoordinates
    df = (
        pd.concat([df2017, df2018, df2019], sort = False)
        [[
            "HarvestYear", 
            "ID2", 
            "Latitude", 
            "Longitude", 
            "SampleID", 
            "Crop", 
            "GrainYieldDryPerArea",
            "GrainYield125PerArea", 
            "BiomassDryPerArea", 
            "GrainTestWeight",
            "GrainMoisture", 
            "GrainProtein",
            "GrainStarch",
            "GrainGluten", 
            "CropExists", 
            "Comments"]]
        .reset_index(drop=True)
    )    

    # Write the file
    dateNow = time.strftime("%Y%m%d")
    outPath = os.sep.join(["output", f"aggregatedYieldBiomass_2017-2019_P2A1_{dateNow}.csv"])
    df.to_csv(outPath, na_rep = "", index = False)

if __name__ == "__main__":
    main()