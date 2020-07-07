import matplotlib.pyplot as plt
import geopandas
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("geo-app").getOrCreate()

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
#world["gdp_per_cap"] = world.gdp_md_est / world.pop_est
world["gdp_per_cap_mean"] = world.loc[:, "gdp_md_est"].mean()
world["gdp_per_cap_stdev"] = world.loc[:, "gdp_md_est"].std()
world["gdp_per_cap_z_score"] = (world.gdp_md_est - world.gdp_per_cap_mean) / world.gdp_per_cap_stdev

ax = world.plot(column="gdp_per_cap_z_score", legend=True, cmap="tab20b", edgecolor="white")

# We can now plot our ``GeoDataFrame``.
gdf.plot(ax=ax, color="red")

plt.show()
