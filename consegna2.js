/*********************************************
 * Corso di Basi di dati 2018/19             *
 * Esercizio Bonus 1                         *
 * Giacomo Gualandi - matricola 657814       *
 * Email - giacomo.gualandi3@studio.unibo.it *
 **********************************************/

conn = new Mongo();
db = conn.getDB("biblioteche");


use culturaBO;
 


/* Punto 1) */
//find trova tutti i documenti nella collezione biblioteche, count() effettua il loro conteggio
cursor1 = db.biblioteche.find().count();
print("[PUNTO 1] Numero di biblioteche presenti: " + cursor1);

/* Punto 2) */
//il find mi seleziona i documenti con il campo quartiere:"Porto - Saragozza", count() effettua il conteggio
cursor2 = db.biblioteche.find({quartiere:"Porto - Saragozza"}).count();
print("[PUNTO 2] Numero di biblioteche : " + cursor2); 
print("");

/* Punto 3) */
//remove elimina i documenti che hanno il campo tipologia="Biblioteca Universitaria"
db.biblioteche.remove({tipologia:"Biblioteca Universitaria"});
cursor3 = db.biblioteche.find().count();
print("[PUNTO 3] Numero di biblioteche presenti dopo filtraggio: " + cursor3);
print("");


print("------------Punto 4-----------------------------------------------------");
/* Punto 4) */
// update mi permette di aggiornare i campi di un documento, con set aggiungo una coppia chiave/valore
db.biblioteche.update({name:"Biblioteca R. Ruffilli"},{$set:{giornoChiusura:"Sabato e Domenica" }});
var cursor = db.biblioteche.find({name:"Biblioteca R. Ruffilli"});
while (cursor.hasNext()) {
	printjson(cursor.next());
};


print("");
print("------------Punto-5-----------------------------------------------------");
/* Punto 5) */
/* Creo una collezione di nome bacheca per inserire i post, con campi _id, post, data, titolo, nome*/
db.createCollection("bacheca");

db.bacheca.insert({_id:"101",post:"Apertura Serale in tutto il mese di Dicembre",data:"1/1/2018",titolo:"Apertura serale",nome:"Biblioteca Borgo Panigale"})
db.bacheca.insert({_id:"201",post:"Apertura straordinaria in data 1/5/2018, visitate le nostre collezioni",data:"20/4/2018",titolo:"Apertura straordinaria",nome:"Biblioteca dell'Archiginnasio"})
db.bacheca.insert({_id:"301",post:"Apertura straordinaria in data 2/6/2018, per mostra temporanea su Antica Roma",data:"21/5/2018",titolo:"Apertura straordinaria",nome:"Biblioteca dell'Archiginnasio"})
db.bacheca.insert({_id:"401",post:"Si avvisa che il museo sarà chiuso durante i we di Agosto",data:"4/8/2018",titolo:"Avviso all'utenza",nome:"Biblioteca dell'Archiginnasio"})
db.bacheca.insert({_id:"501",post:"Esposizione limitata stampe di fine Ottocento",data:"15/7/2018",titolo:"Collezione temporanea",nome:"Biblioteca del Quartiere Navile - Zona Corticella",})
db.bacheca.insert({_id:"601",post:"Chiusura anticipata alle 16",data:"01/8/2018",titolo:"Orario Estivo durante il mese di Agosto",nome:"Biblioteca del Quartiere Navile - Zona Corticella",})

/* Creo una collezione di nome commenti per inserire i commenti, con campi post_id, testo, utente[], data*/
/* Correlata alla collezione bacheca tramite il campo post_id replicato*/
db.createCollection("commenti");

db.commenti.insert({post_id:"101",testo:"Era ora",utente:{nome:"Anonimo",eta:27},data:"10/1/2018"});
db.commenti.insert({post_id:"101",testo:"Molto bello!",utente:{nome:"Michele",cognome:"Rossi",eta:35,condizione:"Studente"},data:"10/1/2018"});

db.commenti.insert({post_id:"201",testo:"Complimenti!",utente:{nome:"Sara",cognome:"Verdi",eta:45,condizione:"Docente"},data:"21/4/2018"});
db.commenti.insert({post_id:"201",testo:"Sicuramente sarò presente!",utente:{nome:"Antonio",cognome:"Rossi",eta:18},data:"21/4/2018"});

db.commenti.insert({post_id:"301",testo:"Quando inizia la mostra?",utente:{nome:"Sara",cognome:"Verdi",eta:45,condizione:"Docente"},data:"22/5/2018"});
db.commenti.insert({post_id:"301",testo:"Mostra bellissima",utente:{nome:"Giulia",cognome:"Bianchi",eta:35},data:"22/5/2018"});
db.commenti.insert({post_id:"301",testo:"Tutto bello, anche se l'allestimento lascia a desiderare",utente:{nome:"Antonio",cognome:"Neri",condizione:"Impiegato"},data:"25/5/2018"});

db.commenti.insert({post_id:"501",testo:"Esiste una rassegna fotografica del materiale esposto?",utente:{nome:"Giacomo",cognome:"Viola",eta:21,condizione:"Studente"},data:"15/8/2018"});
db.commenti.insert({post_id:"501",testo:"Spero duri fino a Settembre",utente:{nome:"Giovanna",cognome:"Neri",eta:38,condizione:"Informatico"},data:"18/8/2018"});

db.commenti.insert({post_id:"601",testo:"Grazie della comunicazione",utente:{nome:"Anonimo",eta:45},data:"10/8/2018"});


print(" ");
print("------------Punto-6-----------------------------------------------------");
/* Punto 6) */
/* Utilizzo un cursore per scorrere la collezione bacheca e con la funzione count conto quanti POST ci sono nella */
/* biblioteca dell'Archiginnasio che hanno titolo = 'Apertura straordinaria' */

cursor6a = db.bacheca.find({nome:"Biblioteca dell'Archiginnasio",titolo:"Apertura straordinaria"}).count();
print("[PUNTO 6] Numero POST nella biblioteca dell'Archiginnasio: " + cursor6a);


print(" ");
print("------------------Punto-7----------------------------------------------");
/* Punto 7) */

// Eseguo un primo ciclo nella collezione bacheca per trovare i documenti (in questo caso post) relativi alla biblioteca dell'Archiginnasio
// il secondo ciclo nella collezione commenti mi permette di contare i documenti (commenti) relativi ai documenti trovati nel primo ciclo
var i = 0; // utilizzo questa variabile come contatore

// Scorro la collezione bacheca e cerco i documenti con campo nome="Biblioteca dell'Archiginnasio"
var cursor7a = db.bacheca.find({nome:"Biblioteca dell'Archiginnasio"});

while (cursor7a.hasNext()){
	bibliotecaAttuale = cursor7a.next();
	cursor7b=db.commenti.find({});
	while (cursor7b.hasNext()){ // scorro la collezione commenti
		commentoAttuale=cursor7b.next();
//		print(commentoAttuale);
		//Controllo della condizione di JOIN
		if(bibliotecaAttuale["_id"]==commentoAttuale["post_id"]){ // incremento di uno se i campi coincidono
			i = i+1;
		}
	}
};

print("[PUNTO 7] Numero Commenti nella biblioteca dell'Archiginnasio : " + i);



print("");
print("---------------Punto-8--------------------------------------------------");
/* Punto 8) */
//aggiungere distinct ???

//var cursor8a = db.commenti.find({post_id:"101"});// questo va
//var cursor8a = db.commenti.find({"utente.eta":{$lte:30}});// funziona
//var cursor8a = db.commenti.find({"utente.condizione":"Studente"}); // funziona
//var cursor8a = db.commenti.find({$or:[{"utente.eta":{$lte:30}},{"utente.condizione":"Studente"}]}); // funziona


//Utilizzo i selettori per filtrare i documenti richiesti dall'esercizio: or è OR booleano, lte lower-then-equal, cioè minore uguale 

var cursor8a = db.commenti.find({$or:[{"utente.eta":{$lte:30}},{"utente.condizione":"Studente"}]},{post_id:1}); 
//var cursor8a = db.commenti.distinct({$or:[{"utente.eta":{$lte:30}},{"utente.condizione":"Studente"}]},{post_id:1});  


//ciclo sui risultati del cursor8a
while(cursor8a.hasNext()){
	utenteAttuale = cursor8a.next();
	idAttuale = utenteAttuale["post_id"];// il campo post_id mi identifica il post nella collezione commenti
	var cursor8b=db.bacheca.find({});
	while (cursor8b.hasNext()){
		bachecaAttuale=cursor8b.next();
		//Controllo della condizione di JOIN
		if(idAttuale==bachecaAttuale["_id"]){ // se i due campi coincidono, estraggo il nome della biblioteca
			bachecaBibliotecaAttuale=bachecaAttuale["nome"];
			cursor8c=db.biblioteche.find();
			while(cursor8c.hasNext()){
				bibliotecaAttuale = cursor8c.next(); // con questo ciclo ottengo i campi richiesti dall'esercizio dalla collezione biblioteche
				if(bibliotecaAttuale["name"]==bachecaBibliotecaAttuale){
					print("[PUNTO 8] Biblioteche recensite da studenti/giovani: " + bibliotecaAttuale["name"] + " - " + bibliotecaAttuale["indirizzo"] + " - " +  bibliotecaAttuale["telefono"]);
				}
			}

		}
	}
};


print("");
print("---------------------------Punto-9------------------------------------");
/* Punto 9) */

//Crea un indice
db.biblioteche.ensureIndex({posizione:"2dsphere"});
var cursor9=db.biblioteche.find({posizione:{ $near:
					{ $geometry:
						{ type: "Point", coordinates:[11.356001, 44.497095]}, // scambiati i valori
						$maxDistance: 2000
					}
				       }					
});//,{name:1,indirizzo:1});

while(cursor9.hasNext()){
	bibliotecaAttuale = cursor9.next();
	print("[PUNTO 9] Biblioteche vicino al DISI: " + bibliotecaAttuale["name"] + " - " + bibliotecaAttuale["indirizzo"]);
};


print("");
print("---------------------------Punto-10------------------------------------");
/* Punto 10) */
//var cursor10 = db.biblioteche.aggregate([{$group:{_id:"$quartiere",totale:{$sum:1}}},{$project:{quartiere:1,sum:1}}]);//mettere il $sum alla fine ? 
var cursor10 = db.biblioteche.aggregate([{$group:{_id:"$quartiere",totale:{$sum:1}}}]); // mettere il $sum alla fine ? 

while(cursor10.hasNext()){
	bibliotecaAttuale = cursor10.next();
//	printjson(bibliotecaAttuale);
	print("[PUNTO 10] Quartiere: " + bibliotecaAttuale["_id"] + " Numero Biblioteche presenti " + bibliotecaAttuale["totale"]);
};



//db.dropDatabase();

