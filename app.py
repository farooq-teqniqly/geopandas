import matplotlib.pyplot as plt
import geopandas
import os

from geopandas import GeoDataFrame
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, col, trim


def load_county_geo_data_file(shape_file: str) -> GeoDataFrame:
    return geopandas.read_file(shape_file)


def load_county_census_data_file(path, spark: SparkSession) -> DataFrame:
    return spark.read.csv(path, header=True, inferSchema=True)


def plot_counties():
    data_folder = os.path.join(os.getcwd(), "data", "tl_2017_us_county")
    shape_file = os.path.join(data_folder, "tl_2017_us_county.shp")

    county_gdf = geopandas.read_file(shape_file)
    wa_counties_gdf = county_gdf[county_gdf.STATEFP == "53"]
    wa_counties_gdf.plot()
    plt.show()


def plot_south_america(spark: SparkSession):
    rows = [
        ("Buenos Aires", "Argentina", -34.58, -58.66),
        ("Brasilia", "Brazil", -15.78, -47.91),
        ("Santiago", "Chile", -33.45, -70.66),
        ("Bogota", "Colombia", 4.60, -74.08),
        ("Caracas", "Venezuela", 10.48, -66.86),
    ]

    cols = ["City", "Country", "Latitude", "Longitude"]

    df = spark.createDataFrame(rows, cols)

    pdf = df.toPandas()

    gdf = geopandas.GeoDataFrame(
        pdf, geometry=geopandas.points_from_xy(pdf.Longitude, pdf.Latitude)
    )

    world = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))

    # We restrict to South America.
    world = world[world.continent == "South America"]
    world["gdp_per_cap_mean"] = world.loc[:, "gdp_md_est"].mean()
    world["gdp_per_cap_stdev"] = world.loc[:, "gdp_md_est"].std()
    world["gdp_per_cap_z_score"] = (
        world.gdp_md_est - world.gdp_per_cap_mean
    ) / world.gdp_per_cap_stdev

    ax = world.plot(
        column="gdp_per_cap_z_score", legend=True, cmap="tab20b", edgecolor="white"
    )

    # We can now plot our ``GeoDataFrame``.
    gdf.plot(ax=ax, color="red")

    plt.show()

def munge_county_census_data_set(df: DataFrame) -> DataFrame:
    df = df.select("STNAME", "CTYNAME", "CENSUS2010POP", "POPESTIMATE2019").where(
        "COUNTY <> 0"
    )

    df = df.withColumn("CTYNAME", trim(regexp_replace(col("CTYNAME"), "County", "")))

    df = df.withColumnRenamed("STNAME", "STATE").withColumnRenamed(
        "CTYNAME", "COUNTY"
    )

    return df

def main():
    spark = SparkSession.builder.master("local[*]").appName("geo-app").getOrCreate()

    data_folder = os.path.join(os.getcwd(), "data")

    census_file = os.path.join(data_folder, "co-est2019-alldata.csv")
    cdf: DataFrame = load_county_census_data_file(census_file, spark)
    cdf = munge_county_census_data_set(cdf)
    assert cdf.count() == 3142

    geo_data_folder = os.path.join(data_folder, "tl_2017_us_county")
    shape_file = os.path.join(geo_data_folder, "tl_2017_us_county.shp")

    gdf: GeoDataFrame = load_county_geo_data_file(shape_file)
    gdf_spark = spark.createDataFrame(gdf).select("NAME", "STATEFP")
    assert gdf_spark.count() == 3233

if __name__ == "__main__":
    main()
