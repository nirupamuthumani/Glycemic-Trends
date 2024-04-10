# Glycemic-Trends
This project explores the potential of wearable devices to detect early signs of prediabetes by monitoring physiological changes and generating digital biomarkers. The data has been collected from participants aged 35-65, with A1C levels of 5.2-6.4%, who were equipped with a Dexcom 6 continuous glucose monitor and an Empatica E4 wristband for 10 days. The devices collected data on glucose levels, heart rate, skin temperature, and more, aiming to identify individuals at risk for prediabetes and hyperglycemia.

The data is from Physionet: https://www.physionet.org/content/big-ideas-glycemic-wearable/1.0.0/
This BIG IDEAs Lab Glycemic Variability and Wearable Device Data is made available under the Open Data Commons Attribution License: http://opendatacommons.org/licenses/by/1.0/

The original data had seven distinct physiological metrics: tri-axial accelerometry (ACC), blood volume pulse (BVP), interstitial glucose concentration (Dexcom), electrodermal activity (EDA), heart rate (HR), interbeat interval (IBI), and skin temperature (TEMP). Each metric was accompanied by a timestamp, providing a granular view of glucose levels, heart activity, physical movement, and more. Additionally, demographic information including gender and HbA1c levels was stored in a separate "Demographics.csv" file.

The data has been aggregated by summarizing key physiological metrics for analysis.

For blood volume pulse (BVP), and electrodermal activity (EDA), the data was condensed to daily median, maximum, and minimum values per patient, to eliminate minute-by-minute variations that don't significantly impact glycemic variability or blood pressure.

Heart rate (HR) data was similarly aggregated to capture only the minimum, mean, and maximum values, which are essential for assessing heart rate changes.

Inter-beat interval (IBI) data was refined to include 30 continuous minutes of daily data for each participant to accurately compute heart rate variability (HRV) using RMSSD (Root Mean Square of the Successive Differences) calculations. It's a measure used in heart rate variability (HRV) analysis to quantify the variability in intervals between successive heartbeats. Specifically, it calculates the variability in the time interval between consecutive heartbeats, known as RR intervals.

We have ignored tri-axial accelerometry (ACC) for this particular analysis. The ERD diagram displays the final structure of the tables.
