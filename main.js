const mongoose = require('mongoose');
const fs = require('fs');
const readline = require('readline');
// Schéma MongoDB pour les données des établissements
const etablissementSchema = new mongoose.Schema({
    siren: String,
    nic: String,
    siret: String,
    dateCreationEtablissement: Date,
    dateDernierTraitementEtablissement: Date,
    typeVoieEtablissement: String,
    libelleVoieEtablissement: String,
    codePostalEtablissement: String,
    libelleCommuneEtablissement: String,
    codeCommuneEtablissement: String,
    dateDebut: Date,
    etatAdministratifEtablissement: String,
});


mongoose.connect('mongodb://127.0.0.1:27017/sirene').then(async () => {
    // Appel de la fonction pour indexer les données depuis le fichier CSV
    indexDataFromCSV('D:/Intech/Semestre10/Big_data/output/output_1.csv');
});

// Fonction pour indexer les données à partir d'un fichier CSV
function indexDataFromCSV(csvFilePath) {
    const Etablissement = mongoose.model('', etablissementSchema, 'etablissement');
    const readInterface = readline.createInterface({
        input: fs.createReadStream(csvFilePath)
    });
    let isFirstLine = true;
    let etablissements = [];
    readInterface.on('line', line => {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }
        const fields = line.split(',');
        // Vérifier si toutes les valeurs requises sont présentes
        if (fields.length < 46) {
            console.log('Ignorer la ligne - Nombre insuffisant de champs');
            readInterface.close()
        }
        // Créer un nouvel objet Etablissement avec les données nécessaires
        const newData = {
            siren: fields[0],
            nic: fields[1],
            siret: fields[2],
            dateCreationEtablissement: fields[4],
            dateDernierTraitementEtablissement: fields[8],
            typeVoieEtablissement: fields[16],
            libelleVoieEtablissement: fields[17],
            codePostalEtablissement: fields[18],
            libelleCommuneEtablissement: fields[19],
            codeCommuneEtablissement: fields[22],
            dateDebut: fields[44],
            etatAdministratifEtablissement: fields[45],
        };
        // Créer un nouvel établissement dans la base de données
        const etablissement = new Etablissement(newData);
        etablissements.push(etablissement);
    })
    // Une fois toutes les lignes lues, insérer les établissements en bulk
    readInterface.on('close', () => {
        console.log(etablissements)
        Etablissement.bulkSave(etablissements)
            .then(() => {
                console.log('Tous les établissements ont été sauvegardés avec succès.');
            })
            .catch(err => {
                console.error('Une erreur est survenue lors de la sauvegarde des établissements :', err);
            });
    });
}