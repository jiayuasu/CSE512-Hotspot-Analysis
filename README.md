# CSE512-Hotspot-Analysis

## HotZoneAnalysis
You need to implement a function that can perform the HotZoneAnalysis. This function is as follows: HotZoneAnalysis(DataFrame RectangleDf, DataFrame PointDf). The first input of the function is a rectangle dataframe which contains only one column named with “rectangleshape”. It is a string type with format “x,y”. The second input is a point dataframe which consists of one column named with “pointshape”. Its type is string and its format is “minx,miny,maxx,maxy”. The output for this function will be another dataframe which stores the number of points in each rectangle. The output dataframe has two columns: (rectangleshape, count). For the output dataframe, its schema needs to be exactly the same with such requirement.
