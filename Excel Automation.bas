' VBA (Visual Basic for Applications): Programming language for automating tasks in Microsoft Office
' These scripts demonstrate advanced automation for Excel reporting and data processing

' 1. Advanced Sales Dashboard Automation
' Creates a comprehensive sales dashboard with pivot tables and charts
Sub GenerateSalesDashboard()
    ' Variable Declaration
    Dim wsData As Worksheet, wsDashboard As Worksheet  ' Worksheet objects
    Dim lastRow As Long, lastCol As Long              ' Last row/column counters
    Dim dataRange As Range, chartObj As ChartObject   ' Range and chart objects
    Dim pivotCache As PivotCache, pivotTable As PivotTable ' Pivot table objects
    
    ' Set worksheet references
    Set wsData = ThisWorkbook.Sheets("SalesData")      ' Source data worksheet
    Set wsDashboard = ThisWorkbook.Sheets("Dashboard") ' Output dashboard worksheet
    
    ' Clear existing dashboard content
    wsDashboard.Cells.Clear
    
    ' Find the last row and column with data in source worksheet
    lastRow = wsData.Cells(wsData.Rows.Count, "A").End(xlUp).Row    ' Last row in column A
    lastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column ' Last column in row 1
    
    ' Define the data range including all cells
    Set dataRange = wsData.Range("A1").Resize(lastRow, lastCol)
    
    ' Create pivot cache (data source for pivot table)
    Set pivotCache = ThisWorkbook.PivotCaches.Create( _
        SourceType:=xlDatabase, _     ' Source is Excel table
        SourceData:=dataRange)        ' Range containing the data
    
    ' Create pivot table in dashboard worksheet
    Set pivotTable = pivotCache.CreatePivotTable( _
        TableDestination:=wsDashboard.Range("B2"), _  ' Location for pivot table
        TableName:="SalesAnalysis")                   ' Name for reference
    
    ' Configure pivot table layout and fields
    With pivotTable
        .PivotFields("Region").Orientation = xlRowField        ' Rows by region
        .PivotFields("ProductCategory").Orientation = xlColumnField ' Columns by category
        .PivotFields("TotalAmount").Orientation = xlDataField  ' Values field
        .DataFields(1).Function = xlSum                       ' Sum aggregation
        .DataFields(1).NumberFormat = "$#,##0"                ' Currency formatting
    End With
    
    ' Create chart object for visualization
    Set chartObj = wsDashboard.ChartObjects.Add( _
        Left:=400, Width:=400, Top:=50, Height:=300)  ' Position and size
    
    ' Configure chart settings
    With chartObj.Chart
        .SetSourceData Source:=pivotTable.TableRange1  ' Use pivot table as data source
        .ChartType = xlColumnClustered                 ' Column chart type
        .HasTitle = True                               ' Enable chart title
        .ChartTitle.Text = "Sales by Region and Category" ' Chart title text
    End With
    
    ' Add key metrics to dashboard
    With wsDashboard
        .Range("B15").Value = "Total Sales:"
        .Range("C15").Formula = "=SUM(SalesData!G:G)"      ' Sum entire G column
        .Range("C15").NumberFormat = "$#,##0"              ' Currency format
        
        .Range("B16").Value = "Average Order Value:"
        .Range("C16").Formula = "=AVERAGE(SalesData!G:G)"  ' Average of G column
        .Range("C16").NumberFormat = "$#,##0"              ' Currency format
        
        .Range("B17").Value = "Number of Orders:"
        .Range("C17").Formula = "=COUNTA(SalesData!A:A)-1" ' Count non-empty cells minus header
    End With
    
    ' Format the metrics area
    With wsDashboard
        .Range("B15:C17").Borders.LineStyle = xlContinuous  ' Add borders
        .Range("B15:C17").Interior.Color = RGB(240, 240, 240) ' Light gray background
    End With
    
    ' Completion message
    MsgBox "Sales dashboard generated successfully!", vbInformation
End Sub

' 2. Automated Data Validation and Cleaning
' Validates data quality and identifies problematic records
Sub CleanAndValidateData()
    Dim ws As Worksheet
    Dim lastRow As Long, i As Long
    Dim invalidCount As Long
    
    Set ws = ThisWorkbook.Sheets("RawData")
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row  ' Find last row
    invalidCount = 0  ' Counter for validation issues
    
    ' Turn off screen updating for performance
    Application.ScreenUpdating = False
    
    ' Add validation status column after last column
    ws.Cells(1, ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column + 1).Value = "ValidationStatus"
    
    ' Loop through each data row (skip header row 1)
    For i = 2 To lastRow
        Dim status As String
        status = "Valid"  ' Default status
        
        ' Validate Email Format
        If Not IsValidEmail(ws.Cells(i, 3).Value) Then
            status = "Invalid Email"
            invalidCount = invalidCount + 1
        End If
        
        ' Validate Phone Number
        If Not IsValidPhone(ws.Cells(i, 4).Value) Then
            status = "Invalid Phone"
            invalidCount = invalidCount + 1
        End If
        
        ' Check for Negative Values in Amount
        If IsNumeric(ws.Cells(i, 5).Value) Then
            If ws.Cells(i, 5).Value < 0 Then
                status = "Negative Amount"
                invalidCount = invalidCount + 1
            End If
        End If
        
        ' Check for Future Dates
        If IsDate(ws.Cells(i, 2).Value) Then
            If ws.Cells(i, 2).Value > Date Then
                status = "Future Date"
                invalidCount = invalidCount + 1
            End If
        End If
        
        ' Write validation status to the new column
        ws.Cells(i, ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column).Value = status
    Next i
    
    ' Apply conditional formatting to highlight invalid records
    With ws.Range(ws.Cells(1, 1), ws.Cells(lastRow, ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column))
        .FormatConditions.Add Type:=xlExpression, Formula1:="=$F2<>""Valid""" ' F is status column
        .FormatConditions(.FormatConditions.Count).Interior.Color = RGB(255, 200, 200) ' Light red
    End With
    
    ' Restore screen updating
    Application.ScreenUpdating = True
    
    ' Show validation summary
    MsgBox "Data validation completed! " & invalidCount & " invalid records found.", vbInformation
End Sub

' Custom function to validate email format using regular expressions
Function IsValidEmail(email As String) As Boolean
    Dim regex As Object
    Set regex = CreateObject("VBScript.RegExp")  ' Create regex object
    
    With regex
        .Pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"  ' Email pattern
        .IgnoreCase = True    ' Case insensitive
        .Global = False       ' Find first match only
    End With
    
    IsValidEmail = regex.Test(email)  ' Test if email matches pattern
End Function

' Custom function to validate phone number format
Function IsValidPhone(phone As String) As Boolean
    Dim cleanPhone As String
    ' Remove common phone number formatting characters
    cleanPhone = Replace(Replace(Replace(Replace(phone, " ", ""), "-", ""), "(", ""), ")", "")
    IsValidPhone = IsNumeric(cleanPhone) And Len(cleanPhone) >= 10  ' Must be numeric and at least 10 digits
End Function

' 3. Automated Report Distribution
' Sends completed reports via email automatically
Sub SendAutomatedReports()
    Dim outlookApp As Object, outlookMail As Object  ' Outlook objects
    Dim wsDashboard As Worksheet, wsData As Worksheet ' Worksheet objects
    Dim tempFilePath As String, recipientEmail As String
    Dim emailSubject As String, emailBody As String
    
    Set wsDashboard = ThisWorkbook.Sheets("Dashboard")
    Set wsData = ThisWorkbook.Sheets("SalesData")
    
    ' Generate temporary PDF file in system temp folder
    tempFilePath = Environ("TEMP") & "\Sales_Report_" & Format(Date, "yyyy-mm-dd") & ".pdf"
    wsDashboard.ExportAsFixedFormat Type:=xlTypePDF, Filename:=tempFilePath
    
    ' Create Outlook application instance
    Set outlookApp = CreateObject("Outlook.Application")
    Set outlookMail = outlookApp.CreateItem(0)  ' 0 = olMailItem
    
    ' Compose email content
    emailSubject = "Daily Sales Report - " & Format(Date, "mmmm d, yyyy")
    emailBody = "Hello," & vbCrLf & vbCrLf & _
                "Please find attached the daily sales report." & vbCrLf & vbCrLf & _
                "Key Highlights:" & vbCrLf & _
                "- Total Sales: " & Format(Application.Sum(wsData.Range("G:G")), "$#,##0") & vbCrLf & _
                "- Orders Processed: " & Application.CountA(wsData.Range("A:A")) - 1 & vbCrLf & _
                "- Top Performing Region: " & GetTopRegion(wsData) & vbCrLf & vbCrLf & _
                "Best regards," & vbCrLf & _
                "Automated Reporting System"
    
    ' Configure and send email
    With outlookMail
        .To = "management@company.com"
        .CC = "sales-team@company.com"
        .Subject = emailSubject
        .Body = emailBody
        .Attachments.Add tempFilePath  ' Attach the PDF report
        .Send  ' Send immediately (use .Display to review first)
    End With
    
    ' Clean up temporary file
    Kill tempFilePath
    
    ' Release object references
    Set outlookMail = Nothing
    Set outlookApp = Nothing
    
    MsgBox "Report sent successfully!", vbInformation
End Sub

' Helper function to determine top performing region
Function GetTopRegion(ws As Worksheet) As String
    Dim lastRow As Long
    lastRow = ws.Cells(ws.Rows.Count, "D").End(xlUp).Row  ' Region column
    
    ' Simplified logic - in practice, you'd use pivot tables or aggregation
    GetTopRegion = "Northeast"  ' Placeholder implementation
End Function

' 4. Dynamic Chart Generator
' Creates multiple charts automatically based on data
Sub CreateDynamicCharts()
    Dim ws As Worksheet, chartSheet As Worksheet
    Dim lastRow As Long, chartObj As ChartObject
    Dim sourceRange As Range
    
    Set ws = ThisWorkbook.Sheets("AnalysisData")
    
    ' Clean up: Delete existing chart sheet if it exists
    On Error Resume Next  ' Ignore error if sheet doesn't exist
    Application.DisplayAlerts = False  ' Suppress confirmation dialog
    ThisWorkbook.Sheets("Charts").Delete
    Application.DisplayAlerts = True
    On Error GoTo 0  ' Restore normal error handling
    
    ' Create new dedicated worksheet for charts
    Set chartSheet = ThisWorkbook.Sheets.Add
    chartSheet.Name = "Charts"
    
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row
    
    ' Create Sales Trend Chart (Line Chart)
    Set sourceRange = ws.Range("A1:B" & lastRow)  ' Date and sales columns
    Set chartObj = chartSheet.ChartObjects.Add(Left:=50, Width:=500, Top:=50, Height:=300)
    
    With chartObj.Chart
        .ChartType = xlLine  ' Line chart for trends
        .SetSourceData Source:=sourceRange
        .HasTitle = True
        .ChartTitle.Text = "Monthly Sales Trend"
        .Axes(xlCategory).HasTitle = True
        .Axes(xlCategory).AxisTitle.Text = "Month"
        .Axes(xlValue).HasTitle = True
        .Axes(xlValue).AxisTitle.Text = "Sales ($)"
    End With
    
    ' Create Product Performance Chart (Bar Chart)
    Set sourceRange = ws.Range("D1:E" & lastRow)  ' Product and revenue columns
    Set chartObj = chartSheet.ChartObjects.Add(Left:=50, Width:=500, Top:=400, Height:=300)
    
    With chartObj.Chart
        .ChartType = xlBarClustered  ' Bar chart for comparisons
        .SetSourceData Source:=sourceRange
        .HasTitle = True
        .ChartTitle.Text = "Product Performance"
        .Axes(xlValue).HasTitle = True
        .Axes(xlValue).AxisTitle.Text = "Products"
        .Axes(xlCategory).HasTitle = True
        .Axes(xlCategory).AxisTitle.Text = "Revenue ($)"
    End With
    
    ' Create Regional Distribution Chart (Pie Chart)
    Set sourceRange = ws.Range("G1:H" & lastRow)  ' Region and percentage columns
    Set chartObj = chartSheet.ChartObjects.Add(Left:=600, Width:=400, Top:=50, Height:=300)
    
    With chartObj.Chart
        .ChartType = xlPie  ' Pie chart for proportions
        .SetSourceData Source:=sourceRange
        .HasTitle = True
        .ChartTitle.Text = "Sales by Region"
        .HasLegend = True
        .Legend.Position = xlLegendPositionRight  ' Legend on right side
    End With
    
    MsgBox "Dynamic charts created successfully!", vbInformation
End Sub

' 5. Data Import and Consolidation from Multiple Files
' Combines data from multiple Excel files into a single dataset
Sub ConsolidateMultipleFiles()
    Dim sourceFolder As String, fileName As String
    Dim targetWorkbook As Workbook, sourceWorkbook As Workbook
    Dim targetSheet As Worksheet, sourceSheet As Worksheet
    Dim lastRow As Long, fileCount As Long
    
    Set targetWorkbook = ThisWorkbook  ' Current workbook
    Set targetSheet = targetWorkbook.Sheets("ConsolidatedData")
    
    ' Clear existing consolidated data
    targetSheet.Cells.Clear
    
    sourceFolder = "C:\SalesData\"  ' Folder containing source files
    fileName = Dir(sourceFolder & "*.xlsx")  ' Get first Excel file
    fileCount = 0  ' Counter for processed files
    
    Application.ScreenUpdating = False  ' Improve performance
    
    ' Add column headers to consolidated sheet
    targetSheet.Range("A1").Value = "File Source"
    targetSheet.Range("B1").Value = "OrderDate"
    targetSheet.Range("C1").Value = "CustomerID"
    targetSheet.Range("D1").Value = "ProductID"
    targetSheet.Range("E1").Value = "Quantity"
    targetSheet.Range("F1").Value = "TotalAmount"
    
    lastRow = 2  ' Start from row 2 (after headers)
    
    ' Loop through all Excel files in the folder
    Do While fileName <> ""
        ' Open source workbook
        Set sourceWorkbook = Workbooks.Open(sourceFolder & fileName)
        Set sourceSheet = sourceWorkbook.Sheets(1)  ' First worksheet
        
        Dim sourceLastRow As Long
        sourceLastRow = sourceSheet.Cells(sourceSheet.Rows.Count, "A").End(xlUp).Row
        
        ' Check if there's data (excluding header)
        If sourceLastRow > 1 Then
            ' Copy data from each column with file source identifier
            With sourceSheet
                .Range("A2:A" & sourceLastRow).Copy  ' OrderDate
                targetSheet.Range("B" & lastRow).PasteSpecial xlPasteValues
                
                .Range("B2:B" & sourceLastRow).Copy  ' CustomerID
                targetSheet.Range("C" & lastRow).PasteSpecial xlPasteValues
                
                .Range("C2:C" & sourceLastRow).Copy  ' ProductID
                targetSheet.Range("D" & lastRow).PasteSpecial xlPasteValues
                
                .Range("D2:D" & sourceLastRow).Copy  ' Quantity
                targetSheet.Range("E" & lastRow).PasteSpecial xlPasteValues
                
                .Range("E2:E" & sourceLastRow).Copy  ' TotalAmount
                targetSheet.Range("F" & lastRow).PasteSpecial xlPasteValues
            End With
            
            ' Add file source identifier for all copied rows
            targetSheet.Range("A" & lastRow & ":A" & (lastRow + sourceLastRow - 2)).Value = fileName
            
            ' Update row counter for next file
            lastRow = lastRow + sourceLastRow - 1
            fileCount = fileCount + 1
        End If
        
        ' Close source workbook without saving changes
        sourceWorkbook.Close SaveChanges:=False
        fileName = Dir()  ' Get next file
    Loop
    
    Application.ScreenUpdating = True
    
    ' Format the consolidated data
    With targetSheet
        .Rows(1).Font.Bold = True  ' Bold headers
        .Columns.AutoFit           ' Auto-fit column widths
        .Range("A1:F1").Interior.Color = RGB(200, 200, 200)  ' Gray header background
    End With
    
    ' Show consolidation summary
    MsgBox "Consolidated " & fileCount & " files successfully! Total records: " & lastRow - 2, vbInformation
End Sub

' 6. Advanced Data Analysis Macros
' Performs comprehensive data analysis with multiple techniques
Sub PerformAdvancedAnalysis()
    Dim ws As Worksheet, analysisSheet As Worksheet
    Dim lastRow As Long, i As Long
    Dim totalSales As Double, avgOrder As Double
    
    Set ws = ThisWorkbook.Sheets("SalesData")
    Set analysisSheet = ThisWorkbook.Sheets("AdvancedAnalysis")
    
    ' Clear previous analysis
    analysisSheet.Cells.Clear
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row
    
    ' Calculate basic business metrics
    totalSales = Application.WorksheetFunction.Sum(ws.Range("G2:G" & lastRow))
    avgOrder = Application.WorksheetFunction.Average(ws.Range("G2:G" & lastRow))
    
    ' Create analysis summary section
    With analysisSheet
        .Range("A1").Value = "Advanced Sales Analysis"
        .Range("A1").Font.Bold = True
        .Range("A1").Font.Size = 14
        
        .Range("A3").Value = "Total Sales:"
        .Range("B3").Value = totalSales
        .Range("B3").NumberFormat = "$#,##0"
        
        .Range("A4").Value = "Average Order Value:"
        .Range("B4").Value = avgOrder
        .Range("B4").NumberFormat = "$#,##0"
        
        .Range("A5").Value = "Number of Orders:"
        .Range("B5").Value = lastRow - 1
        
        .Range("A6").Value = "Analysis Date:"
        .Range("B6").Value = Now
        .Range("B6").NumberFormat = "mmmm d, yyyy h:mm AM/PM"
    End With
    
    ' Call specialized analysis functions
    Call CalculateSalesTrend(ws, analysisSheet, lastRow)
    Call PerformCustomerSegmentation(ws, analysisSheet, lastRow)
    Call AnalyzeProductPerformance(ws, analysisSheet, lastRow)
    
    ' Auto-fit columns for better readability
    analysisSheet.Columns.AutoFit
    
    MsgBox "Advanced analysis completed successfully!", vbInformation
End Sub

' Trend analysis subroutine
Sub CalculateSalesTrend(ws As Worksheet, analysisSheet As Worksheet, lastRow As Long)
    Dim monthRange As Range, salesRange As Range
    Dim uniqueMonths As Collection
    Dim i As Long, monthKey As String
    Dim monthSales As Object
    
    ' Create dictionary for storing monthly sales totals
    Set monthSales = CreateObject("Scripting.Dictionary")
    Set uniqueMonths = New Collection
    
    ' Group sales by month and calculate totals
    For i = 2 To lastRow
        monthKey = Format(ws.Cells(i, 2).Value, "yyyy-mm")  ' Format as Year-Month
        
        If monthSales.Exists(monthKey) Then
            ' Add to existing month total
            monthSales(monthKey) = monthSales(monthKey) + ws.Cells(i, 7).Value
        Else
            ' Initialize new month entry
            monthSales.Add monthKey, ws.Cells(i, 7).Value
            uniqueMonths.Add monthKey
        End If
    Next i
    
    ' Output trend analysis to worksheet
    analysisSheet.Range("A8").Value = "Monthly Sales Trend:"
    analysisSheet.Range("A8").Font.Bold = True
    
    ' Write monthly sales data
    For i = 1 To uniqueMonths.Count
        analysisSheet.Cells(9 + i, 1).Value = uniqueMonths(i)
        analysisSheet.Cells(9 + i, 2).Value = monthSales(uniqueMonths(i))
        analysisSheet.Cells(9 + i, 2).NumberFormat = "$#,##0"
    Next i
End Sub