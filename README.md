# Engine Data Formatter

To use this application: 

1. Define the parameters in the application.yml (make sure the correct clientId and measurementId are being used)
2. Create input file in root directory or specify path in .yml (default is "input.json" in the root directory of this application) 
- run GetScores.sql for Impacts Requests or GetSurveyData.sql for Scores requests (found in src/main/resources)
- save the json as input.json or whatever is configured as the input file name
3. Run the app