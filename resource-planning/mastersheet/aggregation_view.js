// Define the views we want to aggregate
const AGGREGATED_VIEWS = [
    {
      name: 'Personalplanung',
      range: 'A:J',
      lastCol: 10  // Last_Updated column
    },
    {
      name: 'Client_Projects',
      range: 'A:L',
      lastCol: 11  // Last_Updated column
    },
    {
      name: 'Client_Projects_Per_Person',
      range: 'A:Q',
      lastCol: 14  // Last_Updated column
    },
    {
      name: 'Monitoring',
      range: 'A:M',
      lastCol: 11  // Last_Updated column
    }
  ];
  
  function createMasterView(ss, viewConfig, sheetIds) {
    const masterSheetName = `Master_${viewConfig.name}`;
    let masterSheet = ss.getSheetByName(masterSheetName);
    
    // If sheet exists, remove it and create new at the end
    if (masterSheet) {
      ss.deleteSheet(masterSheet);
    }
    masterSheet = ss.insertSheet(masterSheetName, ss.getSheets().length);
    
    // Create the IMPORTRANGE formula with query to remove duplicate headers
    let formula = '=QUERY({';
    
    // Add IMPORTRANGE for each sheet ID
    const importRanges = sheetIds.map(id => 
      `IMPORTRANGE("${id}"; "Aggregated_${viewConfig.name}!${viewConfig.range}")`
    );
    
    formula += importRanges.join(';');
    formula += `}; "select * where Col1 <> 'Year_Month' and Col1 is not null format Col${viewConfig.lastCol} 'yyyy-MM-dd HH:mm:ss'"`;
    
    // Apply formula
    masterSheet.getRange('A1').setFormula(formula);
    
    // Format the sheet
    formatMasterSheet(masterSheet, viewConfig);
  }
  
  function formatMasterSheet(sheet, viewConfig) {
    // Wait a bit for the IMPORTRANGE data to load
    Utilities.sleep(2000);
    
    try {
      // Auto-resize columns if we have data
      if (sheet.getLastRow() > 1) {
        sheet.autoResizeColumns(1, sheet.getLastColumn());
        
        // Add filter
        sheet.getRange(1, 1, sheet.getLastRow(), sheet.getLastColumn())
          .createFilter();
          
        // Format timestamp column
        if (sheet.getLastRow() > 1) {
          sheet.getRange(2, viewConfig.lastCol, sheet.getLastRow() - 1, 1)
            .setNumberFormat('yyyy-MM-dd HH:mm:ss');
        }
        
        // Format number columns (starting from column 5)
        if (sheet.getLastRow() > 1 && sheet.getLastColumn() >= 5) {
          const numberRange = sheet.getRange(2, 5, sheet.getLastRow() - 1, sheet.getLastColumn() - 5);
          numberRange.setNumberFormat('#,##0.00');
        }
      }
    } catch (error) {
      Logger.log(`Error formatting master sheet: ${error.toString()}`);
    }
  }
  
  function createMasterAggregation() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const configSheet = ss.getSheetByName('Config');
    
    if (!configSheet) {
      throw new Error('Please create a Config sheet first');
    }
    
    // Read config data
    const configData = configSheet.getDataRange().getValues();
    const headers = configData[0];
    const sheetIdCol = headers.indexOf('Sheet_ID');
    
    if (sheetIdCol === -1) {
      throw new Error('Config sheet must have a Sheet_ID column');
    }
    
    // Get sheet IDs (skip header row)
    const sheetIds = configData
      .slice(1)
      .map(row => row[sheetIdCol])
      .filter(id => id); // Remove empty values
    
    // Process each type of aggregated view
    AGGREGATED_VIEWS.forEach(viewConfig => {
      try {
        createMasterView(ss, viewConfig, sheetIds);
      } catch (error) {
        Logger.log(`Error processing view ${viewConfig.name}: ${error.toString()}`);
      }
    });
  }
  
  function onOpen() {
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('Master Aggregation')
      .addItem('Create Master Views', 'createMasterAggregation')
      .addToUi();
  }