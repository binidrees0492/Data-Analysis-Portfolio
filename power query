/*
Power Query M is the data transformation language used in Power BI, Excel, and other Microsoft products.
It's functional language that processes data step-by-step through a transformation pipeline.
*/

// 1. Advanced Date Table Generation
// Creates a comprehensive date table for time intelligence calculations
let
    // LET: Defines the start of a query with multiple steps
    // Variables for date range
    StartDate = #date(2020, 1, 1),    // Start date for the calendar
    EndDate = #date(2024, 12, 31),    // End date for the calendar
    
    // Calculate number of days between dates
    NumberOfDays = Duration.Days(EndDate - StartDate),
    
    // Generate list of dates from start to end
    DateList = List.Dates(StartDate, NumberOfDays + 1, #duration(1, 0, 0, 0)),
    
    // IN: Final result that gets returned
    // Convert list to table
    #"Converted to Table" = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}, null, ExtraValues.Error),
    
    // Set proper data type for date column
    #"Changed Type" = Table.TransformColumnTypes(#"Converted to Table",{{"Date", type date}}),
    
    // Add Year column (2020, 2021, etc.)
    #"Added Year" = Table.AddColumn(#"Changed Type", "Year", each Date.Year([Date]), Int64.Type),
    
    // Add Quarter column (Q1, Q2, etc.)
    #"Added Quarter" = Table.AddColumn(#"Added Year", "Quarter", each "Q" & Text.From(Date.QuarterOfYear([Date]))),
    
    // Add Month Name (January, February, etc.)
    #"Added Month Name" = Table.AddColumn(#"Added Quarter", "Month Name", each Date.MonthName([Date]), type text),
    
    // Add Week Number (1-52)
    #"Added Week Number" = Table.AddColumn(#"Added Month Name", "Week Number", each Date.WeekOfYear([Date]), Int64.Type),
    
    // Add Day of Week (Monday, Tuesday, etc.)
    #"Added Day of Week" = Table.AddColumn(#"Added Week Number", "Day of Week", each Date.DayOfWeekName([Date]), type text),
    
    // Add Is Weekend flag (Weekend/Weekday)
    #"Added Is Weekend" = Table.AddColumn(#"Added Day of Week", "Is Weekend", each if Date.DayOfWeek([Date]) = 5 or Date.DayOfWeek([Date]) = 6 then "Weekend" else "Weekday", type text)
in
    #"Added Is Weekend"  // Final output of the query

// 2. Data Cleaning and Transformation
// Comprehensive data cleaning pipeline for raw sales data
let
    Source = Excel.Workbook(File.Contents("C:\Data\Raw_Sales.xlsx"), null, true),
    
    // Access specific worksheet named "Sales"
    Sales_Sheet = Source{[Item="Sales"]}[Data],
    
    // Promote first row to column headers
    #"Promoted Headers" = Table.PromoteHeaders(Sales_Sheet, [PromoteAllScalars=true]),
    
    // Set proper data types for each column
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{
        {"OrderID", type text},        // Text for alphanumeric order IDs
        {"OrderDate", type date},      // Date type for proper date handling
        {"CustomerID", type text},     // Text for customer identifiers
        {"ProductID", type text},      // Text for product codes
        {"Quantity", Int64.Type},      // Whole numbers for quantities
        {"UnitPrice", Currency.Type},  // Currency for monetary values
        {"TotalAmount", Currency.Type} // Currency for calculated totals
    }),
    
    // Remove rows with invalid quantities (negative or zero)
    #"Filtered Rows" = Table.SelectRows(#"Changed Type", each [Quantity] > 0),
    
    // Remove duplicate rows
    #"Removed Duplicates" = Table.Distinct(#"Filtered Rows"),
    
    // Add calculated column for line total
    #"Added Custom" = Table.AddColumn(#"Removed Duplicates", "LineTotal", each [Quantity] * [UnitPrice], Currency.Type),
    
    // Replace any calculation errors with zero
    #"Replaced Errors" = Table.ReplaceErrorValues(#"Added Custom", {{"LineTotal", 0}})
in
    #"Replaced Errors"

// 3. Parameterized Data Source
// Creates a reusable function for loading data from Excel files
let
    // Define a function that takes file path as parameter
    Source = (filePath as text) =>
    let
        FileContent = Excel.Workbook(File.Contents(filePath), null, true),
        DataSheet = FileContent{[Item="SalesData"]}[Data],
        PromotedHeaders = Table.PromoteHeaders(DataSheet, [PromoteAllScalars=true])
    in
        PromotedHeaders
in
    Source  // Returns the function itself

// 4. Multiple File Consolidation
// Combines data from multiple Excel files in a folder
let
    // Get all files from specified folder
    Source = Folder.Files("C:\SalesData\Monthly"),
    
    // Filter out hidden system files
    FilteredHiddenFiles = Table.SelectRows(Source, each [Attributes]?[Hidden]? <> true),
    
    // Keep only Excel files
    ExcelFiles = Table.SelectRows(FilteredHiddenFiles, each Text.EndsWith([Name], ".xlsx")),
    
    // Load data from each Excel file
    AddedCustom = Table.AddColumn(ExcelFiles, "Data", each Excel.Workbook([Content]){[Item="Sheet1"]}[Data]),
    
    // Expand the data from each file into columns
    ExpandedData = Table.ExpandTableColumn(AddedCustom, "Data", Table.ColumnNames(AddedCustom[Data]{0})),
    
    // Remove unnecessary file system columns
    RemovedColumns = Table.RemoveColumns(ExpandedData,{"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes"})
in
    RemovedColumns

// 5. Advanced Error Handling
// Robust data loading with comprehensive error tracking
let
    Source = Excel.Workbook(File.Contents("C:\Data\CustomerData.xlsx"), null, true),
    CustomerData = Source{[Item="Customers"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(CustomerData, [PromoteAllScalars=true]),
    
    // Attempt data type conversion with cultural awareness
    SafeTransform = Table.TransformColumnTypes(PromotedHeaders, {
        {"CustomerID", type text},
        {"CustomerName", type text},
        {"JoinDate", type date},
        {"CreditLimit", Currency.Type}
    }, "en-US"),  // "en-US" specifies US English format for dates/numbers
    
    // Find rows that failed type conversion
    ErrorRows = Table.SelectRowsWithErrors(SafeTransform),
    
    // Keep only clean rows
    CleanData = Table.RemoveRowsWithErrors(SafeTransform),
    
    // Create error log if there are problematic rows
    LogErrors = if Table.RowCount(ErrorRows) > 0 then 
        Table.AddColumn(ErrorRows, "ErrorDescription", each "Data type conversion failed")
    else
        null
in
    CleanData  // Return only clean data for further processing
