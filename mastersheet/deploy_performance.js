// Configuration object defining the tables and their properties
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
      range: 'H39:H63',
      description: 'Custom Range 1'
    },
    {
      range: 'I73:K97',
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
    },
    {
      range: 'G5',
      description: 'Avg Sick Leave Reference Month'
    },
    {
      range: 'B5',
      description: 'Teamlead'
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

// Helper Functions for Sheet Processing
function isMonthSheet(sheetName) {
  Logger.log(`Checking if ${sheetName} is a month sheet`);
  const isMonth = /^\d{4}-\d{2}$/.test(sheetName);
  Logger.log(`Result: ${isMonth}`);
  return isMonth;
}

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

function columnToLetter(column) {
  let temp, letter = '';
  while (column > 0) {
    temp = (column - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    column = (column - temp - 1) / 26;
  }
  return letter;
}

// Function to update a monthly sheet
function updateMonthlySheet(templateSheet, targetSheet) {
  Logger.log(`Updating monthly sheet: ${targetSheet.getName()}`);
  
  // Define ranges that should be blank
  const blankRanges = [
    'B15:B30',
    'B40:B63',
    'B74:B97',
    'C102:D102',
    'B103:D111',
    'B127:B142'
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
      if (rowIndex === 4) {
        return targetSheet.getRange(rowIndex + 1, colIndex + 1).getValue();
      }
      return templateFormulas[rowIndex][colIndex] || value;
    })
  );
  
  // Apply merged data
  targetSheet.getRange('A1:Z1000').setValues(mergedData);
  
  // Clear specific ranges
  blankRanges.forEach(range => {
    targetSheet.getRange(range).clearContent();
  });
  
  // Restore preserved values
  restorePreservedValues(targetSheet, preservedValues);
  
  Logger.log('Monthly sheet update completed');
}

// Function to update monitoring sheet
function updateMonitoringSheet(templateSheet, targetSheet) {
  Logger.log('Updating Project Monitoring sheet');
  
  // Define ranges that should be blank for monitoring sheet
  const blankRanges = [
    'B16:B39',
    'B50:B73',
    'C80:D80',
    'B81:D89',
    'B103:B111'
  ];
  
  // Store preserved values (if any)
  const preservedValues = getPreservedValues(targetSheet, CONFIG.GLOBAL_PRESERVED_RANGES);
  
  // Get template values and properties
  const templateValues = templateSheet.getRange('A1:Z1000').getValues();
  const templateFormulas = templateSheet.getRange('A1:Z1000').getFormulas();
  
  // Create merged array of values and formulas
  const mergedData = templateValues.map((row, rowIndex) => 
    row.map((value, colIndex) => {
      // Skip row 5 completely
      if (rowIndex === 4) {
        return targetSheet.getRange(rowIndex + 1, colIndex + 1).getValue();
      }
      return templateFormulas[rowIndex][colIndex] || value;
    })
  );
  
  // Apply merged data
  targetSheet.getRange('A1:Z1000').setValues(mergedData);
  
  // Clear specific ranges
  blankRanges.forEach(range => {
    targetSheet.getRange(range).clearContent();
  });
  
  // Restore preserved values
  restorePreservedValues(targetSheet, preservedValues);
  
  Logger.log('Project Monitoring sheet update completed');
}

// Function to deploy notes to all team sheets
function deployNotesToTeamSheets() {
  Logger.log('Starting notes deployment process...');
  
  // Get notes from development sheet
  const devSheet = SpreadsheetApp.getActiveSpreadsheet();
  const notesMap = new Map();

  // Process each sheet in the development spreadsheet
  devSheet.getSheets().forEach(sheet => {
    const sheetName = sheet.getName();
    // Skip aggregated views
    if (sheetName.startsWith('Aggregated_')) return;

    Logger.log(`Scanning sheet: ${sheetName} for notes`);

    // Get the actual last row and column
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    
    // Scan entire sheet range
    if (lastRow > 0 && lastCol > 0) {
      const fullRange = sheet.getRange(1, 1, lastRow, lastCol);
      const notes = fullRange.getNotes();
      
      // Store cells with notes
      for (let row = 0; row < notes.length; row++) {
        for (let col = 0; col < notes[row].length; col++) {
          if (notes[row][col] && notes[row][col].trim() !== '') {
            const cellA1Notation = sheet.getRange(row + 1, col + 1).getA1Notation();
            const key = `${sheetName}!${cellA1Notation}`;
            notesMap.set(key, notes[row][col]);
            Logger.log(`Found note in ${key}: "${notes[row][col].substring(0, 50)}..."`);
          }
        }
      }
    }
  });

  if (notesMap.size === 0) {
    SpreadsheetApp.getUi().alert('No notes found in development sheet.');
    return;
  }

  Logger.log(`Found ${notesMap.size} notes to deploy`);

  // Deploy to each team sheet using existing CONFIG
  CONFIG.TARGET_SHEETS.forEach(teamSheet => {
    try {
      const ss = SpreadsheetApp.openById(teamSheet.id);
      Logger.log(`Processing sheet: ${teamSheet.name}`);
      
      // Deploy each note to corresponding cell in target sheet
      notesMap.forEach((note, location) => {
        try {
          const [sheetName, cellRef] = location.split('!');
          const sheet = ss.getSheetByName(sheetName);
          
          if (sheet) {
            const cell = sheet.getRange(cellRef);
            cell.setNote(note);
            Logger.log(`Deployed note to ${teamSheet.name} at ${location}`);
          } else {
            Logger.log(`Sheet ${sheetName} not found in ${teamSheet.name}`);
          }
        } catch (error) {
          Logger.log(`Error deploying note to ${teamSheet.name} at ${location}: ${error.toString()}`);
        }
      });
      
      Logger.log(`Successfully deployed ${notesMap.size} notes to ${teamSheet.name}`);
    } catch (error) {
      Logger.log(`Error accessing ${teamSheet.name}: ${error.toString()}`);
    }
  });
  
  SpreadsheetApp.getUi().alert(`Successfully deployed ${notesMap.size} notes to ${CONFIG.TARGET_SHEETS.length} team sheets.`);
  Logger.log('Notes deployment process completed');
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

// Create menu and add all deployment options
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Deployment')
    .addItem('Deploy to All Team Sheets', 'deployToTeamSheets')
    .addItem('Deploy Notes to Teams', 'deployNotesToTeamSheets')
    .addToUi();
}