// 1. Number of buildings vacant and occupied by someone signifies probability of more crime. Value is analyzed in our case.
df.filter(($"IS THE BUILDING CURRENTLY VACANT OR OCCUPIED?"==="Vacant") && ($"ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)"==="true")).count()
res23: Long = 25506

// 2. Number of buildings vacant due to fire signifies risk. Value is analyzed in our case.
df.filter(($"IS THE BUILDING VACANT DUE TO FIRE?"==="true")).count()
res27: Long = 4541
