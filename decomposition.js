const fs = require('fs');
const readline = require('readline');

// Fonction pour diviser le fichier CSV en fichiers plus petits
async function splitCSV(inputFilePath, outputFolderPath, chunkSize) {
    const inputStream = fs.createReadStream(inputFilePath);
    const lineReader = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity // Pour prendre en charge les fins de ligne Windows (\r\n)
    });

    let fileCount = 1; // Initialise le compteur de fichiers à 1
    let lineCount = 0;
    let headerLine = null;
    let outputFileStream = fs.createWriteStream(`${outputFolderPath}/output_${fileCount}.csv`);

    for await (const line of lineReader) {
        // Si c'est la première ligne, la considérer comme l'en-tête
        if (!headerLine) {
            headerLine = line;
            outputFileStream.write(`${line}\n`); // Écrire l'en-tête dans chaque fichier de sortie
        } else {
            // Écrire chaque ligne dans le fichier de sortie actuel
            outputFileStream.write(`${line}\n`);
            lineCount++;
        }

        // Si le nombre de lignes (hors en-tête) dépasse la taille de morceau spécifiée, passer au fichier suivant
        if (lineCount >= chunkSize) {
            outputFileStream.end(); // Fermer le fichier de sortie actuel
            fileCount++; // Augmente le compteur de fichiers
            lineCount = 0;
            outputFileStream = fs.createWriteStream(`${outputFolderPath}/output_${fileCount}.csv`);
            outputFileStream.write(`${headerLine}\n`); // Réécrire l'en-tête dans le nouveau fichier de sortie
        }
    }

    // Fermer le dernier fichier de sortie
    outputFileStream.end();
    console.log('Splitting complete.');

    return fileCount; // Retourne le nombre total de fichiers créés
}

// Utilisation de la fonction pour diviser le fichier CSV
const inputFilePath = 'D:/Intech/Semestre10/Big_data/StockEtablissement_utf8/StockEtablissement_utf8.csv';
const outputFolderPath = 'D:/Intech/Semestre10/Big_data/output';
const chunkSize = 50000; // Nombre de lignes par fichier de sortie

splitCSV(inputFilePath, outputFolderPath, chunkSize)
    .then(numFiles => console.log(`Fichiers divisés avec succès. Nombre total de fichiers créés : ${numFiles}`))
    .catch(err => console.error('Erreur lors de la division des fichiers :', err));