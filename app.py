import math

import matplotlib.pyplot as plt
import geopandas as gp
import pandas as pd
import os

from geopandas import GeoDataFrame


def get_county_df(state_name: str, census_data_file: str, shape_file: str) -> GeoDataFrame:
    if not state_name:
        raise ValueError("Provide a state name.")

    if not census_data_file:
        raise ValueError("Provide the path to the census data file.")

    if not shape_file:
        raise ValueError("Provide the path to the shape file.")

    census_df = pd.read_csv(census_data_file, header=0, encoding="ISO-8859-1")

    census_df = census_df.loc[
        (census_df["COUNTY"] != 0) & (census_df["STNAME"] == state_name), ["COUNTY", "STATE", "CENSUS2010POP",
                                                                           "POPESTIMATE2019"]]

    census_df.rename(columns={"COUNTY": "COUNTYFP", "STATE": "STATEFP"}, inplace=True)

    fips_code = census_df.iat[0, 1]

    geo_data_frame = gp.read_file(shape_file)
    geo_data_frame = geo_data_frame.astype({"STATEFP": int}).astype({"COUNTYFP": int})
    geo_data_frame = geo_data_frame.loc[geo_data_frame["STATEFP"] == fips_code]
    geo_data_frame = pd.merge(geo_data_frame, census_df, on=["STATEFP", "COUNTYFP"])

    return geo_data_frame


def main():
    state_name = input("Enter a state: ")

    if not state_name:
        state_name = "Washington"

    data_folder = os.path.join(os.getcwd(), "data")
    census_file = os.path.join(data_folder, "co-est2019-alldata.csv")

    geo_data_folder = os.path.join(data_folder, "tl_2017_us_county")
    shape_file = os.path.join(geo_data_folder, "tl_2017_us_county.shp")

    geo_data_frame = get_county_df(state_name, census_file, shape_file)
    geo_data_frame = geo_data_frame.to_crs(epsg=3857)

    geo_data_frame["center"] = geo_data_frame["geometry"].centroid
    geo_data_frame_points = geo_data_frame.copy()
    geo_data_frame_points.set_geometry("center", inplace=True)

    geo_data_frame['coords'] = geo_data_frame['geometry'].apply(lambda x: x.representative_point().coords[:])
    geo_data_frame['coords'] = [coords[0] for coords in geo_data_frame['coords']]

    plot_col = "POPESTIMATE2019"
    cmap = "Oranges"

    fig, ax = plt.subplots(1, figsize=(30, 10))
    geo_data_frame.plot(column=plot_col, cmap=cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True)
    ax.axis("off")

    for idx, row in geo_data_frame.iterrows():
        plt.text(row.coords[0], row.coords[1], s=row["NAME"], horizontalalignment='center')

    plt.show()


if __name__ == "__main__":
    main()
