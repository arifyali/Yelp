### ACS clean up
# refer to https://proximityone.wordpress.com/2016/03/21/tip-of-the-day-median-housing-value-by-zip-code/
# http://api.census.gov/data/2014/acs1/variables.html

ACS_data <- read.csv("/var/folders/l8/w7gpklcj3nj54ww3s71l4fs80000gp/T//Rtmp1yZdN2/data2df814845381")[,-24]
ACS_data[,1] = gsub("\\[","",ACS_data[,1])
ACS_data[,23] = gsub("\\[","",ACS_data[,23])
write.csv(ACS_data, "ACS.csv")