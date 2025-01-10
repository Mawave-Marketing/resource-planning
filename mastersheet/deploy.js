const CONFIG = {
  TEMPLATE_ID: '1OgE3WZcgOeQBaFV1P3yKyouREHQUhlm3rh3CYm1MQaA',
  
  // Template sheet names
  TEMPLATE_SHEETS: {
    MONTH: 'template_month',
    MONITORING: 'Project Monitoring'
  },
  
  // Custom ranges to preserve in each monthly sheet
  MONTHLY_PRESERVED_RANGES: [
    {
      range: 'H35:H46',
      description: 'Custom Range 1'
    },
    {
      range: 'I71:K98',
      description: 'Custom Range 2'
    }
  ],
  
  // Global sheet configurations
  GLOBAL_PRESERVED_RANGES: [
    {
      range: 'E5',
      description: 'Team Name'
    },
    {
      range: 'F5',
      description: 'Department'
    },
    {
      range: 'C5',
      description: 'Planning Month'
    }
  ],
  
  TARGET_SHEETS: [
    {
      id: '1gAeqo3B0D-w9OiysvsG8oYf1FZnbhmcmF6AbPP7S8oM',
      name: 'Kappa Planung – Strang PA',
      department: 'Paid Media',
      team: 'PA'
    },
    {
      id: '1WSNHyq_0wD_gJ76SqT_f931DujYuFiHumOnuHoBvteQ',
      name: 'Kappa Planung – Strang PB',
      department: 'Paid Media',
      team: 'PB'
    },
    {
      id: '1h2_bdHXFYAspkwBNJKs_RqnNoE9emi3hwXYYPAjtr4M',
      name: 'Kappa Planung – Strang PC',
      department: 'Paid Media',
      team: 'PC'
    },
    {
      id: '1VjfY8f7FzpmuRHNXGcbdNQfm6ZoqLljQmTTaAFHAdCU',
      name: 'Kappa Planung – Strang PD',
      department: 'Paid Media',
      team: 'PD'
    },
    {
      id: '1U954MuWUbraUQU0cVKjqrq7Dhl1Sh_lqzcLZAk74pog',
      name: 'Kappa Planung – Strang PE',
      department: 'Paid Media',
      team: 'PE'
    }
  ]
};

// Helper function to check if sheet name matches YYYY_MM pattern
function isMonthSheet(sheetName) {
  Logger.log(`Checking if ${sheetName} is a month sheet`);
  const isMonth = /^\d{4}_\d{2}$/.test(sheetName);
  Logger.log(`Result: ${isMonth}`);
  return isMonth;
}

// Helper function to get preserved values from a range
function getPreservedValues(sheet, ranges) {
  Logger.log('Getting preserved values for ranges:');
  const preservedValues = {};
  ranges.forEach(range => {
    try {
      Logger.log(`Getting values for range: ${range.range}`);
      preservedValues[range.range] = sheet.getRange(range.range).getValues();
      Logger.log(`Successfully got values for ${range.range}`);
    } catch (error) {
      Logger.log(`Error getting preserved values from ${range.range}: ${error.message}`);
    }
  });
  return preservedValues;
}

// Helper function to restore preserved values
function restorePreservedValues(sheet, preservedValues) {
  Logger.log('Restoring preserved values');
  for (const [range, values] of Object.entries(preservedValues)) {
    try {
      Logger.log(`Restoring values to range: ${range}`);
      sheet.getRange(range).setValues(values);
      Logger.log(`Successfully restored values to ${range}`);
    } catch (error) {
      Logger.log(`Error restoring values to ${range}: ${error.message}`);
    }
  }
}

// Function to update a monthly sheet
function updateMonthlySheet(templateSheet, targetSheet) {
  Logger.log(`Updating monthly sheet: ${targetSheet.getName()}`);
  
  // Define ranges that should be blank
  const blankRanges = [
    'B16:B26',
    'B36:B46',
    'B72:B98',
    'B104:B111',
    'B127:B135'
  ];
  
  // Store preserved values
  const preservedValues = getPreservedValues(targetSheet, CONFIG.MONTHLY_PRESERVED_RANGES);
  
  // Get template values and properties
  const templateValues = templateSheet.getRange('A1:Z1000').getValues();
  const templateFormulas = templateSheet.getRange('A1:Z1000').getFormulas();
  
  // Create merged array of values and formulas
  const mergedData = templateValues.map((row, rowIndex) => 
    row.map((value, colIndex) => {
      // Skip row 5 completely
      if (rowIndex === 4) { // 0-based index, so 4 is row 5
        return targetSheet.getRange(rowIndex + 1, colIndex + 1).getValue();
      }
      
      // Handle blank ranges
      const cellA1 = columnToLetter(colIndex + 1) + (rowIndex + 1);
      const shouldBeBlank = blankRanges.some(range => 
        isInRange(cellA1, range)
      );
      
      if (shouldBeBlank) {
        return '';
      }
      
      return templateFormulas[rowIndex][colIndex] || value;
    })
  );
  
  // Apply merged data
  targetSheet.getRange('A1:Z1000').setValues(mergedData);
  
  // Restore preserved values
  restorePreservedValues(targetSheet, preservedValues);
  
  Logger.log('Monthly sheet update completed');
}

// Helper function to convert column number to letter
function columnToLetter(column) {
  let temp, letter = '';
  while (column > 0) {
    temp = (column - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    column = (column - temp - 1) / 26;
  }
  return letter;
}

// Helper function to check if a cell is within a range
function isInRange(cellA1, rangeA1) {
  const [startCol, startRow] = rangeA1.split(':')[0].match(/([A-Z]+)(\d+)/).slice(1);
  const [endCol, endRow] = rangeA1.split(':')[1].match(/([A-Z]+)(\d+)/).slice(1);
  const [cellCol, cellRow] = cellA1.match(/([A-Z]+)(\d+)/).slice(1);
  
  return cellCol >= startCol && cellCol <= endCol && 
         parseInt(cellRow) >= parseInt(startRow) && 
         parseInt(cellRow) <= parseInt(endRow);
}

// Function to update monitoring sheet
function updateMonitoringSheet(templateSheet, targetSheet) {
  Logger.log('Updating Project Monitoring sheet');
  
  // Define ranges that should be blank for monitoring sheet
  const blankRanges = [
    'B16:B41',
    'B52:B62',
    'B73:B80',
    'B94:B102'
  ];
  
  // Get template values and properties
  const templateValues = templateSheet.getRange('A1:Z1000').getValues();
  const templateFormulas = templateSheet.getRange('A1:Z1000').getFormulas();
  
  // Create merged array of values and formulas
  const mergedData = templateValues.map((row, rowIndex) => 
    row.map((value, colIndex) => {
      // Skip row 5 completely
      if (rowIndex === 4) { // 0-based index, so 4 is row 5
        return targetSheet.getRange(rowIndex + 1, colIndex + 1).getValue();
      }
      
      // Handle blank ranges
      const cellA1 = columnToLetter(colIndex + 1) + (rowIndex + 1);
      const shouldBeBlank = blankRanges.some(range => 
        isInRange(cellA1, range)
      );
      
      if (shouldBeBlank) {
        return '';
      }
      
      return templateFormulas[rowIndex][colIndex] || value;
    })
  );
  
  // Apply merged data
  targetSheet.getRange('A1:Z1000').setValues(mergedData);
  
  Logger.log('Project Monitoring sheet update completed');
}

// Main deployment function
function deployToTeamSheets() {
  Logger.log('Starting deployment process...');
  
  try {
    const templateSheet = SpreadsheetApp.openById(CONFIG.TEMPLATE_ID);
    Logger.log('Successfully opened template sheet');
    
    const monthTemplate = templateSheet.getSheetByName(CONFIG.TEMPLATE_SHEETS.MONTH);
    const monitoringTemplate = templateSheet.getSheetByName(CONFIG.TEMPLATE_SHEETS.MONITORING);
    
    Logger.log(`Found month template: ${monthTemplate ? 'Yes' : 'No'}`);
    Logger.log(`Found monitoring template: ${monitoringTemplate ? 'Yes' : 'No'}`);
    
    if (!monthTemplate || !monitoringTemplate) {
      throw new Error('Required template sheets not found');
    }
    
    // Process each target sheet
    CONFIG.TARGET_SHEETS.forEach(target => {
      try {
        Logger.log(`\nProcessing target sheet: ${target.name}`);
        const targetSpreadsheet = SpreadsheetApp.openById(target.id);
        Logger.log('Successfully opened target spreadsheet');
        
        // Log all sheets in target
        const sheets = targetSpreadsheet.getSheets();
        Logger.log(`Found ${sheets.length} sheets in target:`);
        sheets.forEach(s => Logger.log(`- ${s.getName()}`));
        
        // Process monthly sheets and monitoring sheet
        sheets.forEach(sheet => {
          const sheetName = sheet.getName();
          Logger.log(`Checking sheet: ${sheetName}`);
          
          if (isMonthSheet(sheetName)) {
            Logger.log(`Updating monthly sheet: ${sheetName}`);
            updateMonthlySheet(monthTemplate, sheet);
            Logger.log(`Finished updating ${sheetName}`);
          }
          
          if (sheetName === CONFIG.TEMPLATE_SHEETS.MONITORING) {
            updateMonitoringSheet(monitoringTemplate, sheet);
          }
        });
        
        Logger.log(`Successfully completed updates for: ${target.name}`);
        
      } catch (error) {
        Logger.log(`Error processing ${target.name}: ${error.message}`);
        Logger.log(`Stack trace: ${error.stack}`);
      }
    });
    
    Logger.log('Deployment process completed');
    
  } catch (error) {
    Logger.log(`Fatal error in deployment: ${error.message}`);
    Logger.log(`Stack trace: ${error.stack}`);
  }
}

// Create menu
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Deployment')
    .addItem('Deploy to All Team Sheets', 'deployToTeamSheets')
    .addToUi();
}