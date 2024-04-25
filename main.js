const fs = require('fs');
const readline = require('readline');
const pm2 = require('pm2');

const inputFilePath = 'D:/Intech/Semestre10/Big_data/StockEtablissement_utf8/StockEtablissement_utf8.csv';
const outputFolderPath = 'D:/Intech/Semestre10/Big_data/output';
const chunkSize = 50000; // Nombre de lignes par fichier de sortie
let fileCount = 1; // Initialise le compteur de fichiers à 1
let workingFileCount = 1; //Commence à un comme le fichier

// Fonction pour diviser le fichier CSV en fichiers plus petits
async function splitCSV(inputFilePath, outputFolderPath, chunkSize) {
    const inputStream = fs.createReadStream(inputFilePath);
    const lineReader = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity // Pour prendre en charge les fins de ligne Windows (\r\n)
    });

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

function putToWorkOrKill(workerId) {
    setTimeout(() => {
        if (workingFileCount <= fileCount) {
            const packet = {
                type: 'numéro du fichier',
                topic: 'process:msg',
                data: {fileNumber: workingFileCount}
            }
            pm2.sendDataToProcessId(workerId, packet, err => {
                if (err) throw err;
                console.log("Envoie du chunk " + packet.data.fileNumber + " au worker n°" + workerId + ".");
            });
            workingFileCount++;
        } else {
            pm2.delete(worker, (err, res) => {
                if (err) {
                    console.error(`Erreur lors de l'arrêt du worker ${worker}:`, err);
                } else {
                    console.log(`Worker ${worker} arrêté avec succès.`);
                }
            });
        }
    }, 1000)
}

splitCSV(inputFilePath, outputFolderPath, chunkSize)
    .then(numFiles => {
        console.log(`Fichiers divisés avec succès. Nombre total de fichiers créés : ${numFiles}`);
        // Démarrer les workers avec PM2
        pm2.connect((err) => {
            if (err) {
                console.error(err)
                process.exit(2)
            }
            pm2.start({
                script: 'worker.js',
                name: 'Worker',
                autorestart: false,
                instances: "10",
                exec_mode: 'cluster'
            }, (err, workers) => setTimeout(() => {
                if (err) {
                    console.error(err);
                    pm2.disconnect();
                } else {
                    for (let worker of workers) {
                        putToWorkOrKill(worker.pm2_env.pm_id);
                        pm2.launchBus((err, pm2_bus) => {
                            if (err) {
                                console.error(err);
                                pm2.disconnect();
                            }
                            console.log("Reception d'un message ");
                            pm2_bus.on('process:msg', (packet) => {
                                if (packet.data.success) {
                                    putToWorkOrKill(packet.process.pm_id);
                                }
                            })
                        })
                    }
                }
            }, 1000));

        });
    })
    .catch(err => console.error('Erreur lors de la division des fichiers :', err));