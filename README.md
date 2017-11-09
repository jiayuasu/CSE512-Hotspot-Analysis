# CSE512-Hotspot-Analysis

## HotZoneAnalysis
You need to implement a function that can perform the HotZoneAnalysis. 

This function is as follows: HotZoneAnalysis(DataFrame RectangleDf, DataFrame PointDf). The first input of the function is a rectangle dataframe which contains only one column named with “rectangleshape”. It is a string type with format “x,y”. The second input is a point dataframe which consists of one column named with “pointshape”. Its type is string and its format is “minx,miny,maxx,maxy”. 

The output for this function will be another dataframe which stores the number of points in each rectangle. The output dataframe has two columns: (rectangleshape, count). For the output dataframe, its schema needs to be exactly the same with such requirement. The dataframe also needs to be sorted according to the rectangleshape column.

The data files needed for this task is located in the src/resources folder. We have already implemented the function to read these two files and generate the required dataframes for calling your function. What you need to do is just implement the HotZoneAnalysis function in the HotzoneUtils.scala file. Your result will be stored into a txt file and compared with the correct answer. So make sure that your result file has the correct sequence of all the rectangle shapes.
