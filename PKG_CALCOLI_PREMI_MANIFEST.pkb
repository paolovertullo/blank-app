CREATE OR REPLACE PACKAGE BODY UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST
AS
   c_debug   CONSTANT BOOLEAN := TRUE;

   -- metti TRUE se vuoi il debug attivo



   FUNCTION FN_CALCOLO_PREMI_MANIFEST (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      l_risultati   t_tabella_premi;
      v_dummy       NUMBER;
      v_forced      BOOLEAN := FALSE;
      v_rec         UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST.t_premio_rec;
   BEGIN
      -- Questo metodo è richiamato come API dal BackEnd  NON PUO FARE UPDATE

      --TODO :   inserisco la logica che se non ci sono i risultati in nume_piazzamento allora restituisco la SIM
      --         se ci sono prendo piazzamento e il dato premio_masaf se è valorizzato
      --        se non è valorizzato allora due strade o chiamo il popola premio_masaf oppure lo calcolo con handler
      -- Prova a selezionare se esiste almeno un NUME_PIAZZAMENTO NULL

      v_dummy := FN_GARA_PREMIATA_MASAF (p_gara_id);

      DBMS_OUTPUT.PUT_LINE (
         '---  FN_CALCOLO_PREMI_MANIFEST ---' || v_dummy || '---');

      IF v_dummy = 0
      THEN
         v_rec.cavallo_id := -1;
         v_rec.nome_cavallo := 'Gara non premiata da MASAF.';
         v_rec.posizione := -1;
         v_rec.note := 'Gara non premiata da MASAF.';
         v_rec.premio := -1;

         PIPE ROW (v_rec);

         RETURN;
      END IF;


      BEGIN
         SELECT 1
           INTO v_dummy
           FROM TC_DATI_CLASSIFICA_ESTERNA
          WHERE     FK_SEQU_ID_DATI_GARA_ESTERNA = p_gara_id
                AND NUME_PIAZZAMENTO IS NULL
                AND ROWNUM = 1;

         -- Se trovi almeno un risultato a null, fai vedere la simulazione
         --perchè classifica non ultimata
         FOR rec
            IN (SELECT *
                  FROM TABLE (FN_CALCOLO_PREMI_MANIFEST_SIM (p_gara_id, 10)))
         LOOP
            PIPE ROW (rec);
         END LOOP;

         RETURN;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;                   -- Nessun piazzamento NULL, non fare nulla
      END;

      l_risultati := t_tabella_premi ();

      FOR rec IN (SELECT fk_sequ_id_cavallo AS cavallo_id,
                         desc_cavallo AS nome_cavallo,
                         nume_piazzamento AS posizione,
                         '' AS note,
                         importo_masaf_calcolato AS premio
                    FROM tc_dati_classifica_esterna
                   WHERE FK_SEQU_ID_DATI_GARA_ESTERNA = p_gara_id)
      LOOP
         v_rec.cavallo_id := rec.cavallo_id;
         v_rec.nome_cavallo := rec.nome_cavallo;
         v_rec.posizione := rec.posizione;
         v_rec.note := rec.note;
         v_rec.premio := rec.premio;

         IF rec.premio IS NULL
         THEN
            v_rec.nome_cavallo := rec.nome_cavallo || ' ( elabora premio )';
         END IF;

         l_risultati.EXTEND;
         l_risultati (l_risultati.COUNT) := v_rec;
      END LOOP;

      FOR i IN 1 .. l_risultati.COUNT
      LOOP
         PIPE ROW (l_risultati (i));
      END LOOP;



      -- attenzione che il CALCOLO_PREMI_MANIFESTAZIONE  porta all'update della classifica_esterna !!!!!
      --CALCOLO_PREMI_MANIFESTAZIONE (p_gara_id, l_risultati);

      --ELABORA_PREMI_GARA (p_gara_id,v_forced,l_risultati);

      --Pipe dei risultati
      --FOR i IN 1 .. l_risultati.COUNT
      --LOOP
      --   PIPE ROW (l_risultati (i));
      --END LOOP;

      RETURN;
   END;



   FUNCTION FN_CALCOLO_PREMI_MANIFEST_SIM (p_gara_id        IN NUMBER,
                                           p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      l_risultati    t_tabella_premi := t_tabella_premi ();
      v_disciplina   VARCHAR2 (50);
   BEGIN
      -- Questo metodo è richiamato come API dal BackEnd

      -- Recupero la disciplina della gara
      v_disciplina := GET_DISCIPLINA (p_gara_id);

      -- Dispatch verso l¿handler della disciplina corretta
      CASE v_disciplina
         WHEN 4
         THEN
            FOR rec
               IN (SELECT *
                     FROM TABLE (
                             FN_CALCOLO_SALTO_OSTACOLI_SIM (p_gara_id,
                                                            p_num_partenti)))
            LOOP
               PIPE ROW (rec);
            END LOOP;
         WHEN 2
         THEN
            FOR rec
               IN (SELECT *
                     FROM TABLE (
                             FN_CALCOLO_ENDURANCE_SIM (p_gara_id,
                                                       p_num_partenti)))
            LOOP
               PIPE ROW (rec);
            END LOOP;
         WHEN 6
         THEN
            FOR rec
               IN (SELECT *
                     FROM TABLE (
                             FN_CALCOLO_DRESSAGE_SIM (p_gara_id,
                                                      p_num_partenti)))
            LOOP
               PIPE ROW (rec);
            END LOOP;
         WHEN 1
         THEN
            FOR rec
               IN (SELECT *
                     FROM TABLE (
                             FN_CALCOLO_ALLEVATORIALE_SIM (p_gara_id,
                                                           p_num_partenti)))
            LOOP
               PIPE ROW (rec);
            END LOOP;
         WHEN 3
         THEN
            FOR rec
               IN (SELECT *
                     FROM TABLE (
                             FN_CALCOLO_COMPLETO_SIM (p_gara_id,
                                                      p_num_partenti)))
            LOOP
               PIPE ROW (rec);
            END LOOP;
         WHEN 7
         THEN
            FOR rec
               IN (SELECT *
                     FROM TABLE (
                             FN_CALCOLO_MONTA_DA_LAVORO_SIM (p_gara_id,
                                                             p_num_partenti)))
            LOOP
               PIPE ROW (rec);
            END LOOP;
         ELSE
            RAISE_APPLICATION_ERROR (
               -20001,
               'Disciplina non gestita: ' || v_disciplina);
      END CASE;

      RETURN;
   END;

   FUNCTION FN_DESC_TIPOLOGICA (p_codice IN NUMBER)
      RETURN VARCHAR2
   IS
      v_descrizione   VARCHAR2 (200);
   BEGIN
      SELECT descrizione
        INTO v_descrizione
        FROM TD_MANIFESTAZIONE_TIPOLOGICHE
       WHERE id = p_codice;

      RETURN v_descrizione;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;             --'Codice sconosciuto (' || p_codice || ')';
   END;


   FUNCTION FN_GARA_PREMIATA_MASAF (p_id_gara_esterna IN NUMBER)
      RETURN NUMBER
   IS
      v_dati_gara               TC_DATI_GARA_ESTERNA%ROWTYPE;
      v_disciplina_id           NUMBER;
      v_categoria_desc          VARCHAR2 (255);
      -- v_tipo_evento_desc        VARCHAR2(255); -- Non sembra più necessario se usiamo v_dati_gara.desc_nome_gara_esterna per il contesto evento
      v_nome_gara               TC_DATI_GARA_ESTERNA.desc_nome_gara_esterna%TYPE; -- Usiamo direttamente il campo della vista
      v_nome_manifestazione     VARCHAR2 (255);
      l_eta_cavalli             NUMBER;                  -- Per l'età numerica
      l_desc_formula            VARCHAR2 (50);
      l_debug_disciplina_desc   VARCHAR2 (100);
      l_premiata                PLS_INTEGER := 0;
   BEGIN
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_id_gara_esterna);

      -- Recupero riga dalla tabella gara_esterna
      SELECT UPPER (mf.desc_denom_manifestazione), UPPER (mf.desc_formula)
        INTO v_nome_manifestazione, l_desc_formula
        FROM TC_DATI_GARA_ESTERNA dg
             JOIN
             TC_DATI_EDIZIONE_ESTERNA ee
                ON ee.sequ_id_dati_edizione_esterna =
                      DG.FK_SEQU_ID_DATI_EDIZ_ESTERNA
             JOIN TC_EDIZIONE ed
                ON ed.sequ_id_edizione = EE.FK_SEQU_ID_EDIZIONE
             JOIN TC_MANIFESTAZIONE mf
                ON mf.sequ_id_manifestazione = ed.fk_sequ_id_manifestazione
       WHERE dg.SEQU_ID_DATI_GARA_ESTERNA = p_id_gara_esterna;



      v_disciplina_id := GET_DISCIPLINA (p_id_gara_esterna);
      v_nome_gara := UPPER (v_dati_gara.desc_nome_gara_esterna);

      IF v_dati_gara.fk_codi_eta IS NOT NULL
      THEN
         l_eta_cavalli :=
            TO_NUMBER (
               SUBSTR (FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_eta), 0, 1));
      ELSE
         l_eta_cavalli :=
            CASE
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '1 ANNO') > 0     -- Aggiunto prima per specificità
               THEN
                  1
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '2 ANNI') > 0     -- Aggiunto prima per specificità
               THEN
                  2
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '3 ANNI') > 0                                --ANNI
               THEN
                  3
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '4 ANNI') > 0                                --ANNI
               THEN
                  4
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '5 ANNI') > 0                                --ANNI
               THEN
                  5
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '6 ANNI') > 0                                --ANNI
               THEN
                  6
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           '7 ANNI') > 0                                --ANNI
               THEN
                  7
               ELSE
                  NULL
            END;
      END IF;


      IF v_dati_gara.FK_CODI_CATEGORIA IS NOT NULL
      THEN
         v_categoria_desc :=
            UPPER (FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA));
      ELSE
         v_categoria_desc :=
            CASE
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           'ELITE') > 0                                 --ANNI
               THEN
                  'ELITE'
               WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                           'SPORT') > 0                                 --ANNI
               THEN
                  'SPORT'
               ELSE
                  NULL
            END;
      END IF;

      CASE v_disciplina_id
         WHEN 1
         THEN
            l_debug_disciplina_desc := 'Circuito Allevatoriale';
            l_premiata := 0;                     -- Inizializza a non premiata

            -- Trasformiamo il nome della gara/categoria in qualcosa di più facile da confrontare
            -- Assumiamo che v_nome_gara possa contenere varianti come "PROVA MORFO-ATTITUDINALE", "MORFO ATTITUDINALE", ecc.
            -- Oppure, se v_categoria_desc (da FK_CODI_CATEGORIA) fosse più standardizzato per le prove, potremmo usarlo.
            -- Per ora, lavoriamo con v_nome_gara.

            -- REGOLA 1: FINALE NAZIONALE (Fieracavalli Verona)
            IF    UPPER (v_nome_manifestazione) LIKE '%FINALI NAZIONALI%'
               OR UPPER (v_nome_manifestazione) LIKE '%FIERACAVALLI VERONA%'
            THEN
               IF    (v_nome_gara LIKE '%MORFO%' AND l_eta_cavalli IN (2, 3))
                  OR                          -- Morfo-Attitudinale 2 e 3 anni
                     (    v_nome_gara LIKE '%ATTITUDINE AL SALTO%'
                      AND l_eta_cavalli = 2)
                  OR                             -- Attitudine al Salto 2 anni
                     (v_nome_gara LIKE '%OBBEDIENZA%' AND l_eta_cavalli = 3)
                  OR                          -- Obbedienza ed Andature 3 anni
                     (    (   v_nome_gara LIKE '%SALTO IN LIBERT%QUALIFIC%' 
                           OR v_nome_gara LIKE '%QUALIFIC%SALTO IN LIBERT%' 
                           OR v_nome_gara LIKE'%FINALE%SALTO IN LIBERT%' 
                           OR v_nome_gara LIKE'%SALTO IN LIBERT%FINALE%' 
                           )
                               AND (l_eta_cavalli IS NULL OR l_eta_cavalli = 3)) 
                  OR                                -- Salto in Libertà 3 anni
                     (v_nome_gara LIKE '%COMBINATA%' AND l_eta_cavalli = 3) -- Classifica Combinata 3 anni
               THEN
                  l_premiata := 1;
               END IF;
            END IF;

            -- REGOLA 2: PREMI REGIONALI ED INTERREGIONALI
            IF     l_premiata = 0
               AND (   UPPER (v_nome_manifestazione) LIKE
                          '%PREMIO REGIONALE%'
                    OR UPPER (v_nome_manifestazione) LIKE
                          '%PREMIO INTERREGIONALE%')
            THEN
               IF    (    v_nome_gara LIKE '%MORFO%'
                      AND l_eta_cavalli IN (1, 2, 3))
                  OR (    v_nome_gara LIKE '%ATTITUDINE AL SALTO%'
                      AND l_eta_cavalli = 2)
                  OR (v_nome_gara LIKE '%OBBEDIENZA%' AND l_eta_cavalli = 3)
                  OR (    (    v_nome_gara LIKE '%SALTO IN LIBERT%QUALIFIC%' 
                           OR v_nome_gara LIKE '%QUALIFIC%SALTO IN LIBERT%' 
                           OR v_nome_gara LIKE'%FINALE%SALTO IN LIBERT%' 
                           OR v_nome_gara LIKE'%SALTO IN LIBERT%FINALE%' 
                           )
                               AND (l_eta_cavalli IS NULL OR l_eta_cavalli = 3)) 
                  OR (v_nome_gara LIKE '%COMBINATA%' AND l_eta_cavalli = 3) -- Classifica Combinata 3 anni
               THEN
                  l_premiata := 1;
               END IF;
            END IF;

            -- REGOLA 3: RASSEGNE FOALS (regionali)
            IF     l_premiata = 0
               AND UPPER (v_nome_manifestazione) LIKE '%FOALS%'
            THEN
               -- Il disciplinare a pag. 18 parla di "Rassegne Foals" come prova morfo-attitudinale.
               -- L'età dei foals è 0 (nati nell'anno). Se l_eta_cavalli = 1 è per puledri di 1 anno, allora
               -- i foals potrebbero avere l_eta_cavalli = 0 o essere identificati diversamente.
               -- Assumendo che i foals abbiano l_eta_cavalli = 0 (o la categoria sia "FOALS")
               IF v_nome_gara LIKE '%MORFO%' OR v_nome_gara LIKE '%FOALS%' -- o l_eta_cavalli = 0 (se 0 è l'età dei foals)
               THEN
                  l_premiata := 1;
               END IF;
            END IF;

            -- REGOLA 4: TAPPE DI PREPARAZIONE
            -- Questa è la regola più generica, quindi va controllata per ultima.
            -- Se la manifestazione non è una Finale, un Premio Regionale/Interr. o una Rassegna Foals,
            -- allora potrebbe essere una Tappa di Preparazione.
            IF     l_premiata = 0
               AND UPPER (v_nome_manifestazione) NOT LIKE
                      '%FINALI NAZIONALI%'
               AND UPPER (v_nome_manifestazione) NOT LIKE
                      '%FIERACAVALLI VERONA%'
               AND UPPER (v_nome_manifestazione) NOT LIKE
                      '%PREMIO REGIONALE%'
               AND UPPER (v_nome_manifestazione) NOT LIKE
                      '%PREMIO INTERREGIONALE%'
               AND UPPER (v_nome_manifestazione) NOT LIKE '%RASSEGNA FOALS%'
            THEN
               IF    (    v_nome_gara LIKE '%MORFO%'
                      AND l_eta_cavalli IN (1, 2, 3))
                  OR                                -- Tappe: Morfo 1,2,3 anni
                     (v_nome_gara LIKE '%OBBEDIENZA%' AND l_eta_cavalli = 3)
                  OR (    (   v_nome_gara LIKE '%SALTO IN LIBERT%QUALIFIC%' 
                           OR v_nome_gara LIKE '%QUALIFIC%SALTO IN LIBERT%' 
                           OR v_nome_gara LIKE'%FINALE%SALTO IN LIBERT%' 
                           OR v_nome_gara LIKE'%SALTO IN LIBERT%FINALE%' 
                           )
                               AND (l_eta_cavalli IS NULL OR l_eta_cavalli = 3))       -- Tappe: Salto Lib. 3 anni
               -- Attitudine al Salto non sembra avere tappe di preparazione, solo Premi Reg. e Finale.
               THEN
                  l_premiata := 1;
               END IF;
            END IF;
         WHEN 2
         THEN
            l_debug_disciplina_desc := 'Endurance';
            l_premiata := 0;                     -- Inizializza a non premiata

            -- REGOLA 1: CAMPIONATO MASAF 7-8 ANNI (TAPPA UNICA)
            -- Il disciplinare (pag. 7) lo chiama "CAMPIONATO MASAF 7-8 ANNI"
            -- e la categoria premiata è "CEI 2**".
            IF (UPPER(TRIM(v_nome_manifestazione)) = 'CAMPIONATO MASAF 7-8 ANNI') -- Nome preciso come da disciplinare
            THEN
               -- Verifichiamo se la gara è la categoria specifica CEI 2**
               -- Adatta il LIKE al formato esatto del nome gara per CEI2** (es. '%CEI2**%', '%CEI 2**%', '%CEI2 STELLE%', ecc.)
               IF (v_nome_gara LIKE '%CEI2\*\*%') OR (v_nome_gara LIKE '%CEI 2\*\*%') OR (v_nome_gara LIKE '%CEI2 STELLE%')
               THEN
                  -- Considera anche l'età se disponibile, anche se la manifestazione è già specifica
                  IF l_eta_cavalli IS NULL OR l_eta_cavalli IN (7, 8) THEN
                     l_premiata := 1;
                  END IF;
               END IF;
            END IF;

            -- NESSUN PREMIO MASAF DIRETTO PER SINGOLE GARE DEL "CIRCUITO MASAF DI ENDURANCE"
            -- (né per le 6 tappe, né per la Tappa di Chiusura).
            -- Il montepremi per il Circuito è ANNUALE e basato su punteggi accumulati (Disciplinare pag. 5).
            -- Quindi, la funzione deve restituire 0 per le gare di manifestazioni come
            -- "CIRCUITO MASAF DI ENDURANCE" o "TAPPA DI CHIUSURA DEL CIRCUITO GIOVANI CAVALLI DI ENDURANCE".
            -- Non è necessario un blocco IF per impostare l_premiata a 1 per queste,
            -- perché di default l_premiata è 0, e questo è il comportamento corretto.
         WHEN 3
         THEN
            l_debug_disciplina_desc := 'Concorso Completo';
            l_premiata := 0;                     -- Inizializza a non premiata

            DECLARE
               v_nome_manifest_normalizzato   VARCHAR2 (255)
                  := UPPER (TRIM (v_nome_manifestazione));
            BEGIN
               -- REGOLA 1: MANIFESTAZIONI "CAMPIONATO 6 ANNI DI COMPLETO" e "CAMPIONATO 7 ANNI DI COMPLETO"
               -- Queste manifestazioni sono le finali per le rispettive età e sono premiate MASAF.
               -- Assumiamo che le gare all'interno di queste manifestazioni specifiche per quell'età siano quelle del campionato.
               IF v_nome_manifest_normalizzato =
                     'CAMPIONATO 6 ANNI DI COMPLETO'
               THEN
                  IF l_eta_cavalli = 6
                  THEN
                     -- Qui potremmo anche controllare se v_nome_gara CONTIENE "CAMPIONATO"
                     -- ma se la manifestazione è già specifica, dovrebbe essere sufficiente l'età.
                     l_premiata := 1;
                  END IF;
               ELSIF v_nome_manifest_normalizzato =
                        'CAMPIONATO 7 ANNI DI COMPLETO'
               THEN
                  IF l_eta_cavalli = 7
                  THEN
                     l_premiata := 1;
                  END IF;
               END IF;

               -- REGOLA 2: MANIFESTAZIONE "FINALE CIRCUITO MASAF DI COMPLETO" (per 4 e 5 anni)
               IF     l_premiata = 0
                  AND v_nome_manifest_normalizzato =
                         'FINALE CIRCUITO MASAF DI COMPLETO'
               THEN
                  -- Le categorie premiate sono per 4 anni (sport/élite) e 5 anni (sport/élite)
                  -- (cfr. Disciplinare Completo, pag. 11, Montepremi Finali Circuito MASAF)
                  IF l_eta_cavalli IN (4, 5)
                  THEN
                     -- Il disciplinare a pag. 11 parla di montepremi per:
                     -- Cavalli di 4 anni e 5 anni categoria élite
                     -- Cavalli di 5 anni categoria sport
                     -- Cavalli 4 anni categoria sport
                     -- Quindi, se l'età è 4 o 5, e la categoria è SPORT o ELITE, è premiata.
                     IF    INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                  'ELITE') > 0
                        OR INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                  'SPORT') > 0
                     THEN
                        l_premiata := 1;
                     END IF;
                  END IF;
               END IF;

               -- REGOLA 3: MANIFESTAZIONE "TROFEO DEL CAVALLO ITALIANO" (Finale del Trofeo)
               IF     l_premiata = 0
                  AND v_nome_manifest_normalizzato =
                         'TROFEO DEL CAVALLO ITALIANO'
               THEN
                  -- Il montepremi MASAF è per la *Finale* del Trofeo (pag. 12).
                  -- Non sono specificate categorie di età o livello per il premio della finale del Trofeo,
                  -- si presume che la gara sia la "finale" stessa.
                  -- Se ci sono più gare in una manifestazione "TROFEO DEL CAVALLO ITALIANO",
                  -- dovremmo identificare quale è effettivamente la "FINALE" del Trofeo.
                  -- Per ora, se la manifestazione ha questo nome, assumiamo che la gara sia la finale premiata.
                  -- Un controllo su v_nome_gara LIKE '%FINALE%' potrebbe essere utile se non tutte le gare
                  -- di questa manifestazione sono la finale.
                  IF    v_nome_gara LIKE '%FINALE%'
                     OR v_nome_gara LIKE '%TROFEO%'
                  THEN         -- Aggiunto per essere più specifici sulla gara
                     l_premiata := 1;
                  END IF;
               END IF;

               -- REGOLA 4: MANIFESTAZIONE "CIRCUITO MASAF COMPLETO" (Tappe per 4 e 5 anni)
               IF     l_premiata = 0
                  AND v_nome_manifest_normalizzato =
                         'CIRCUITO MASAF COMPLETO'
               THEN
                  -- Le tappe del circuito sono per cavalli di 4 e 5 anni (pag. 7 disciplinare)
                  -- e "ogni categoria avrà un Montepremi".
                  IF l_eta_cavalli IN (4, 5)
                  THEN
                     -- Non c'è distinzione sport/élite per il premio di tappa nel disciplinare a pag. 7.
                     -- Quindi, se l'età è 4 o 5, è una tappa premiata.
                     l_premiata := 1;
                  END IF;
               END IF;

               -- REGOLA 5: ESCLUSIONE MANIFESTAZIONE "CAMPIONATO DEL MONDO"
               IF v_nome_manifest_normalizzato = 'CAMPIONATO DEL MONDO'
               THEN
                  l_premiata := 0;
               END IF;

               IF v_nome_gara LIKE '%FISE%'
               THEN
                  l_premiata := 0;
               END IF;
            END;                                        -- Fine blocco declare
         WHEN 4
         THEN                                                -- SALTO OSTACOLI
            l_debug_disciplina_desc := 'Salto Ostacoli';

            -- REGOLA 1: CSIO ROMA / PIAZZA DI SIENA (Master Talent è incluso)
            IF    UPPER (v_nome_manifestazione) LIKE '%CSIO ROMA%'
               OR UPPER (v_nome_manifestazione) LIKE '%SIENA%'
            THEN
               -- Il documento dice "MASTER TALENT GIOVANI CAVALLI PIAZZA DI SIENA 2025 riservato a cavalli di 6 e 7 anni."
               -- E "€ 20.000,00 a carico MASAF per i cavalli italiani"
               -- Assumiamo che se la manifestazione è CSIO Roma e l'età è 6 o 7, è premiata.
               -- La FK_CODI_CATEGORIA dovrebbe aiutare a identificare se è una categoria del Master Talent.
               -- Se il Master Talent ha una sua FK_CODI_CATEGORIA specifica (es. "TALENT") o se si identifica solo per nome manifestazione + età.
               -- Per ora, semplifichiamo: se è CSIO Roma/P.Siena ed è per 6/7 anni, è potenzialmente premiata.
               IF l_eta_cavalli IN (6, 7)
               THEN
                  l_premiata := 1;
               END IF;
            END IF;

            -- REGOLA 2: FINALI CIRCUITO CLASSICO (Campionato e Criterium)
            -- (cfr. Doc "Salto Ostacoli Disciplinare...", pag. 18 e montepremi pag. 26-28)
            IF     l_premiata = 0
               AND UPPER (v_nome_manifestazione) LIKE
                      '%FINALI CIRCUITO CLASSICO%'
            THEN
               -- Le finali hanno montepremi MASAF. Le categorie sono Campionati (4,5,6,7,8+ anni) e Criterium (4,5,6,7,8+ anni)
               -- v_categoria_desc potrebbe essere "CAMPIONATO" o "CRITERIUM" o i documenti usano il nome della gara direttamente.
               -- Per ora, assumo che se la manifestazione è Finale CC, è premiata (la categoria specifica è implicita).
               -- Dovremmo verificare se v_categoria_desc o un altro campo specifica "CAMPIONATO" o "CRITERIUM"
               l_premiata := 1;
            END IF;

            -- REGOLA 3: TAPPE CIRCUITO CLASSICO (Montepremi MASAF per "Alto Livello")
            -- (cfr. Doc "Salto Ostacoli Disciplinare...", pag. 5)
            IF     l_premiata = 0
               AND UPPER (v_nome_manifestazione) LIKE
                      '%CIRCUITO CLASSICO MASAF%'
            THEN
               -- Categorie "Alto Livello" (5,6,7 anni) sono premiate MASAF (pag. 5 Disciplinare S.O.)
               IF v_categoria_desc = 'ALTO' AND l_eta_cavalli IN (5, 6, 7)
               THEN
                  l_premiata := 1;
               -- Categorie "Alto Livello e Selezione" (5,6,7 anni) sono premiate MASAF (pag. 5 Disciplinare S.O.)
               ELSIF     v_categoria_desc = 'SELEZIONE'
                     AND l_eta_cavalli IN (5, 6, 7)
               THEN
                  l_premiata := 1;
               -- Categorie "Elite" (come da tua indicazione "7 ANNI ELITE GP MISTA" sotto CC MASAF)
               -- Queste sono probabilmente le "Elite MASAF" (4,5,6,7 anni) del doc "Circuito FISE/MASAF GC"
               -- che si svolgono sotto l'ombrello del "Circuito Classico MASAF".
               ELSIF     v_categoria_desc = 'ELITE'
                     AND l_eta_cavalli IN (4, 5, 6, 7)
               THEN
                  l_premiata := 1;
               END IF;
            END IF;
         WHEN 6
         THEN
            l_debug_disciplina_desc := 'Dressage';
            l_premiata := 0;                     -- Inizializza a non premiata

            DECLARE
               v_nome_manifest_normalizzato   VARCHAR2 (255)
                  := UPPER (TRIM (v_nome_manifestazione));
            -- Per il dressage, il nome della gara è cruciale per identificare la ripresa e la giornata
            BEGIN
               -- REGOLA 1: TAPPE DEL "CIRCUITO MASAF DI DRESSAGE"
               -- Premiate solo le gare della seconda giornata.
               IF v_nome_manifest_normalizzato = 'CIRCUITO MASAF DI DRESSAGE'
               THEN
                  IF     l_eta_cavalli = 4
                     AND v_nome_gara LIKE '%FEI GIOVANI CAVALLI 4 ANNI%'
                  THEN
                     -- Nelle tappe, la 2° giornata ha la stessa ripresa della 1°. Serve un modo per distinguere 1°/2° giornata.
                     -- Il tuo elenco nomi gare ha "4ANNI - 1°PROVA MASAF" e "4ANNI - 2°PROVA MASAF". Useremo questo.
                     IF    v_nome_gara LIKE '%2°PROVA%'
                        OR v_nome_gara LIKE '%SECONDA GIORNATA%'
                     THEN
                        l_premiata := 1;
                     END IF;
                  ELSIF     l_eta_cavalli = 5
                        AND v_nome_gara LIKE
                               '%FEI FINALE GIOVANI CAVALLI 5 ANNI%'
                  THEN
                     l_premiata := 1;
                  ELSIF     l_eta_cavalli = 6
                        AND v_nome_gara LIKE
                               '%FEI FINALE GIOVANI CAVALLI 6 ANNI%'
                  THEN
                     l_premiata := 1;
                  ELSIF     l_eta_cavalli IN (7, 8)
                        AND v_nome_gara LIKE
                               '%FEI FINALE GIOVANI CAVALLI 7 ANNI%'
                  THEN
                     l_premiata := 1;
                  END IF;

                  -- Un modo più generico se i nomi gara contengono "2°PROVA" o "FINALE" per le tappe:
                  IF     l_eta_cavalli IN (4, 5, 6, 7, 8)
                     AND (   v_nome_gara LIKE '%2°PROVA%'
                          OR (    v_nome_gara LIKE '%FINALE%'
                              AND l_eta_cavalli IN (5, 6, 7, 8))
                          OR -- "FINALE" per 5,6,7,8 anni nella 2a giornata tappe
                             (    v_nome_gara LIKE
                                     '%FEI GIOVANI CAVALLI 4 ANNI%'
                              AND l_eta_cavalli = 4
                              AND v_nome_gara LIKE '%2°PROVA%') -- Specifico per 4 anni
                                                                )
                  THEN
                     -- Potrebbe essere necessario un controllo più granulare sui nomi esatti delle riprese della seconda giornata
                     -- Esempio dai tuoi dati: "4ANNI - 2°PROVA MASAF" è premiata
                     -- "6ANNI FINALE - 2°PROVA MASAF" è premiata
                     -- "MASAF - CAVALLI DI 6 ANNI - F.E.I. FINALE GIOVANI CAVALLI 6 ANNI IN VIGORE" è premiata
                     IF (   v_nome_gara LIKE '%2°PROVA%'
                         OR v_nome_gara LIKE
                               '%F.E.I. FINALE GIOVANI CAVALLI%')
                     THEN
                        l_premiata := 1;
                     END IF;
                  END IF;
               END IF;

               -- REGOLA 2: "FINALE CIRCUITO MASAF DI DRESSAGE"
               -- Qui sono premiati i Campionati (somma di 2 prove).
               -- Se la funzione deve dire se la SINGOLA GARA è premiata, allora nessuna lo è direttamente.
               -- Se la funzione deve dire se la GARA FA PARTE di un campionato premiato, allora sì.
               -- Assumo la seconda interpretazione: la gara è una delle due che compongono un campionato premiato.
               IF     l_premiata = 0
                  AND v_nome_manifest_normalizzato =
                         'FINALE CIRCUITO MASAF DI DRESSAGE'
               THEN
                  IF     l_eta_cavalli = 4
                     AND v_nome_gara LIKE '%FEI GIOVANI CAVALLI 4 ANNI%'
                  THEN
                     l_premiata := 1;    -- Entrambe le prove del Camp. 4 anni
                  ELSIF     l_eta_cavalli = 5
                        AND (   v_nome_gara LIKE
                                   '%FEI PRELIMINARY GIOVANI CAVALLI 5 ANNI%'
                             OR v_nome_gara LIKE
                                   '%FEI FINALE GIOVANI CAVALLI 5 ANNI%')
                  THEN
                     l_premiata := 1;    -- Entrambe le prove del Camp. 5 anni
                  ELSIF     l_eta_cavalli = 6
                        AND (   v_nome_gara LIKE
                                   '%FEI PRELIMINARY GIOVANI CAVALLI 6 ANNI%'
                             OR v_nome_gara LIKE
                                   '%FEI FINALE GIOVANI CAVALLI 6 ANNI%')
                  THEN
                     l_premiata := 1;    -- Entrambe le prove del Camp. 6 anni
                  ELSIF     l_eta_cavalli IN (7, 8)
                        AND (   v_nome_gara LIKE
                                   '%FEI PRELIMINARY GIOVANI CAVALLI 7 ANNI%'
                             OR v_nome_gara LIKE
                                   '%FEI FINALE GIOVANI CAVALLI 7 ANNI%')
                  THEN
                     l_premiata := 1;  -- Entrambe le prove del Camp. 7/8 anni
                  END IF;

                  -- Dobbiamo considerare i nomi gara che hai fornito tipo:
                  -- "2°PROVA CAMPIONATO 5 ANNI FINAL" -> l_eta_cavalli=5, v_nome_gara LIKE '%CAMPIONATO%5 ANNI%FINAL%'
                  -- "MASAF - CAVALLI DI 6 ANNI -F.E.L. PRELIMINARY GIOVANI CAVALLI 6 ANNI IN VIGORE" (in una Finale)
                  -- "MASAF - CAVALLI DI 6 ANNI - F.E.I. FINALE GIOVANI CAVALLI 6 ANNI IN VIGORE" (in una Finale)
                  -- "5ANNI PRELIMINARY - 1°PROVA MASAF E CAMP. IT"
                  -- "6ANNI FINALE - 2°PROVA MASAF E CAMP. IT"
                  IF     (   v_nome_gara LIKE '%MASAF%'
                          OR v_nome_gara LIKE '%CAMP.%')
                     AND         -- Indica che è una gara del campionato MASAF
                         (l_eta_cavalli IN (4, 5, 6, 7, 8))
                  THEN
                     l_premiata := 1;
                  END IF;
               END IF;

               -- REGOLA 3: ESCLUSIONE CAMPIONATO DEL MONDO (VERDEN)
               IF    UPPER (v_nome_manifestazione) LIKE
                        '%CAMPIONATO DEL MONDO%'
                  OR UPPER (v_nome_manifestazione) LIKE '%VERDEN%'
               THEN
                  l_premiata := 0;
               END IF;
            END;                                        -- Fine blocco declare
         WHEN 7
         THEN
            l_debug_disciplina_desc := 'Monta da Lavoro';
            l_premiata := 0;                     -- Inizializza a non premiata

            DECLARE
               v_nome_manifest_normalizzato   VARCHAR2 (255)
                  := UPPER (TRIM (v_nome_manifestazione));
               v_nome_gara_normalizzato       VARCHAR2 (255) := v_nome_gara; -- v_nome_gara è già UPPER nella procedura principale
            BEGIN
               -- Controlla se la manifestazione è una Tappa o la Finale del Circuito Monta da Lavoro
               IF    v_nome_manifest_normalizzato =
                        'CIRCUITO MONTA DA LAVORO'
                  OR v_nome_manifest_normalizzato =
                        'FINALE CIRCUITO MONTA DA LAVORO'
               THEN
                  -- Ora controlla se la categoria della gara è una di quelle premiate MASAF
                  -- Utilizziamo i nomi che hai fornito e li mappiamo alle categorie del disciplinare
                  IF    v_nome_gara_normalizzato = 'ESORDIENTI'
                     OR v_nome_gara_normalizzato = 'CATEGORIA 1'
                     OR                                               -- Cat.1
                       v_nome_gara_normalizzato = 'DEBUTTANTI'
                     OR v_nome_gara_normalizzato = 'CATEGORIA 2'
                     OR                                               -- Cat.2
                       v_nome_gara_normalizzato = 'AMATORI'
                     OR v_nome_gara_normalizzato = 'CATEGORIA 3'
                     OR                                               -- Cat.3
                       v_nome_gara_normalizzato = 'JUNIORES'
                     OR v_nome_gara_normalizzato = 'CATEGORIA 4'
                     OR                                               -- Cat.4
                       v_nome_gara_normalizzato = 'OPEN' -- Cat.5 (già allineato)
                  THEN
                     l_premiata := 1;
                  END IF;
               END IF;
            END;                                        -- Fine blocco declare
         ELSE
            l_debug_disciplina_desc :=
               'ID Sconosciuto ' || TO_CHAR (v_disciplina_id);
            l_premiata := 0;
      END CASE;


      IF v_dati_gara.FK_CODI_TIPO_CLASSIFICA = 77
      THEN
         l_premiata := 0;     -- Non premiata per tipo classifica ADDESTRATIVA
      END IF;

      IF UPPER (l_desc_formula) LIKE '%FISE%'
      THEN
         l_premiata := 0;    --Non premiate perchè sono le manifestazioni FISE
      END IF;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('--- DEBUG FN_GARA_PREMIATA_MASAF ---');
         DBMS_OUTPUT.PUT_LINE ('ID Gara Esterna: ' || p_id_gara_esterna);
         DBMS_OUTPUT.PUT_LINE (
            'Nome Manifestazione (dalla vista): ' || v_nome_manifestazione);
         DBMS_OUTPUT.PUT_LINE (
               'Disciplina: '
            || l_debug_disciplina_desc
            || ' (ID: '
            || v_disciplina_id
            || ')');
         DBMS_OUTPUT.PUT_LINE (
            'Categoria Gara (da FK_CODI_CATEGORIA): ' || v_categoria_desc);
         DBMS_OUTPUT.PUT_LINE (
            'Età cavalli (stringa originale): ' || v_dati_gara.fk_codi_eta);
         DBMS_OUTPUT.PUT_LINE (
            'Età cavalli (numerica estratta): ' || l_eta_cavalli);
         DBMS_OUTPUT.PUT_LINE ('Risultato Premiata MASAF: ' || l_premiata);
         DBMS_OUTPUT.PUT_LINE ('------------------------------------');
      END IF;

      RETURN l_premiata;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
                  'NO_DATA_FOUND in FN_GARA_PREMIATA_MASAF for Gara ID: '
               || p_id_gara_esterna);
         END IF;

         RETURN 0;
      WHEN OTHERS
      THEN
         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
                  'Error in FN_GARA_PREMIATA_MASAF: '
               || SQLERRM
               || ' for Gara ID: '
               || p_id_gara_esterna);
         END IF;

         RETURN 0;
   END FN_GARA_PREMIATA_MASAF;

 
FUNCTION FN_INCENTIVO_MASAF_GARA_FISE (
    p_id_gara_esterna IN NUMBER
) RETURN NUMBER
IS
    v_dati_gara             TC_DATI_GARA_ESTERNA%ROWTYPE;
    v_disciplina_id         NUMBER;
    v_nome_gara_upper       VARCHAR2(500);
    v_nome_manifest_upper   VARCHAR2(500);
    v_livello_tecnico_gara  VARCHAR2(50) := NULL; -- Es. 'H145_SUPERIORE', 'ALTO_LIVELLO_CC_MASAF'
    v_is_estero             BOOLEAN      := FALSE;
    l_tipo_incentivo_gara   PLS_INTEGER  := 0; -- 0 = Nessuno, 1 = Classifica Aggiunta SO, 2 = Incentivo 10%
    l_num_cav_masaf_incentivabili NUMBER := 0;

BEGIN
    -- 1. Ottieni dettagli gara
    v_dati_gara := FN_INFO_GARA_ESTERNA(p_id_gara_esterna);
    v_disciplina_id := GET_DISCIPLINA(p_id_gara_esterna);
    v_nome_gara_upper := UPPER(v_dati_gara.desc_nome_gara_esterna);
    
    -- Recupero riga dalla tabella gara_esterna
      SELECT UPPER (mf.desc_denom_manifestazione)
        INTO v_nome_manifest_upper
        FROM TC_DATI_GARA_ESTERNA dg
             JOIN
             TC_DATI_EDIZIONE_ESTERNA ee
                ON ee.sequ_id_dati_edizione_esterna =
                      DG.FK_SEQU_ID_DATI_EDIZ_ESTERNA
             JOIN TC_EDIZIONE ed
                ON ed.sequ_id_edizione = EE.FK_SEQU_ID_EDIZIONE
             JOIN TC_MANIFESTAZIONE mf
                ON mf.sequ_id_manifestazione = ed.fk_sequ_id_manifestazione
       WHERE dg.SEQU_ID_DATI_GARA_ESTERNA = p_id_gara_esterna;


    -- 2. Determina informazioni aggiuntive sulla gara (livello, luogo)
    -- Questa logica va adattata ai dati reali disponibili
    IF v_disciplina_id = 4 THEN -- Salto Ostacoli
        IF INSTR(v_nome_gara_upper, '145') > 0 OR INSTR(v_nome_gara_upper, '150') > 0 OR
           INSTR(v_nome_gara_upper, '155') > 0 OR INSTR(v_nome_gara_upper, '160') > 0 OR
           INSTR(v_nome_gara_upper, 'GP') > 0 OR INSTR(v_nome_gara_upper, 'GRAND PRIX') >0
           -- Considera anche il livello tecnico se disponibile in v_dati_gara.LIVELLO_GARA ad esempio
        THEN
            v_livello_tecnico_gara := 'H145_SUPERIORE';
        END IF;

        IF v_nome_manifest_upper LIKE '%CIRCUITO CLASSICO MASAF%' AND
           ( UPPER(FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA)) LIKE '%ALTO LIVELLO%' OR v_nome_gara_upper LIKE '%ALTO LIVELLO%')
        THEN
            v_livello_tecnico_gara := 'ALTO_LIVELLO_CC_MASAF';
        END IF;
        
        -- Determina se la gara è all'estero
        -- Ipotizziamo che il nome della manifestazione o un campo specifico lo indichi
        IF INSTR(v_nome_manifest_upper, 'ESTERO') > 0 OR
           INSTR(v_nome_manifest_upper, 'OPGLABBEEK') > 0 -- Aggiungere pattern per località estere
           -- OR v_dati_gara.NAZIONE_EVENTO != 'ITA' -- Se hai un campo nazione
        THEN
            v_is_estero := TRUE;
        END IF;
    END IF;

    -- 3. Logica specifica per disciplina per determinare il TIPO di incentivo della GARA
    CASE v_disciplina_id
        WHEN 4 THEN -- SALTO OSTACOLI (ID 4)
            IF v_livello_tecnico_gara = 'ALTO_LIVELLO_CC_MASAF' AND NOT v_is_estero THEN -- Classifica aggiunta solo in Italia
                l_tipo_incentivo_gara := 1; -- Tipo incentivo: Classifica aggiunta
            ELSIF v_livello_tecnico_gara = 'H145_SUPERIORE' THEN
                l_tipo_incentivo_gara := 2; -- Tipo incentivo: 10% del premio vinto (Italia o Estero)
            END IF;

        WHEN 3 THEN -- CONCORSO COMPLETO (ID 3)
            l_tipo_incentivo_gara := 2; -- Tipo incentivo: 10% del premio vinto

        WHEN 6 THEN -- DRESSAGE (ID 6)
            l_tipo_incentivo_gara := 2; -- Tipo incentivo: 10% del premio vinto
        
        WHEN 7 THEN -- MONTA DA LAVORO (ID 7)
            l_tipo_incentivo_gara := 0; -- Il programma MASAF è il suo circuito. Nessun incentivo AGGIUNTO a gare FITETREC generiche.

        WHEN 2 THEN -- ENDURANCE (ID 2)
            l_tipo_incentivo_gara := 0; -- Nessun incentivo specifico menzionato per gare FISE generiche

        WHEN 1 THEN -- CIRCUITO ALLEVATORIALE (ID 1)
            l_tipo_incentivo_gara := 0; -- Gare già MASAF

        ELSE
            l_tipo_incentivo_gara := 0;
    END CASE;

    -- 4. Se la GARA è di un tipo idoneo a incentivi, conta i cavalli MASAF partecipanti
    IF l_tipo_incentivo_gara > 0 THEN
        BEGIN
            SELECT COUNT(*)
            INTO l_num_cav_masaf_incentivabili
            FROM TC_DATI_CLASSIFICA_ESTERNA cl
            WHERE cl.fk_sequ_id_dati_gara_esterna = p_id_gara_esterna
              AND cl.fk_sequ_id_cavallo IS NOT NULL; -- Cavallo è MASAF
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_num_cav_masaf_incentivabili := 0;
            WHEN OTHERS THEN
                 IF c_debug THEN
                    DBMS_OUTPUT.PUT_LINE('FN_INCENTIVO_MASAF_GARA_FISE: Errore conteggio cavalli per Gara ID ' || p_id_gara_esterna || ': ' || SQLERRM);
                END IF;
                l_num_cav_masaf_incentivabili := 0; -- In caso di errore nel conteggio, restituisce 0
        END;
    END IF;

    IF c_debug THEN
        DBMS_OUTPUT.PUT_LINE('--- DEBUG FN_INCENTIVO_MASAF_GARA_FISE ---');
        DBMS_OUTPUT.PUT_LINE('ID Gara: ' || p_id_gara_esterna);
        DBMS_OUTPUT.PUT_LINE('Disciplina ID: ' || v_disciplina_id);
        DBMS_OUTPUT.PUT_LINE('Nome Manifestazione: ' || v_nome_manifest_upper);
        DBMS_OUTPUT.PUT_LINE('Nome Gara: ' || v_nome_gara_upper);
        DBMS_OUTPUT.PUT_LINE('Categoria Gara (da FK): ' || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA));
        DBMS_OUTPUT.PUT_LINE('Livello Tecnico Gara (dedotto): ' || v_livello_tecnico_gara);
        DBMS_OUTPUT.PUT_LINE('Gara Estero (dedotto): ' || CASE WHEN v_is_estero THEN 'SI' ELSE 'NO' END);
        DBMS_OUTPUT.PUT_LINE('Tipo Incentivo Gara (flag): ' || l_tipo_incentivo_gara);
        DBMS_OUTPUT.PUT_LINE('Num. Cavalli MASAF Incentivabili: ' || l_num_cav_masaf_incentivabili);
        DBMS_OUTPUT.PUT_LINE('------------------------------------------');
    END IF;

    RETURN l_num_cav_masaf_incentivabili;

EXCEPTION
    WHEN OTHERS THEN
        IF c_debug THEN
            DBMS_OUTPUT.PUT_LINE('ERRORE in FN_INCENTIVO_MASAF_GARA_FISE per Gara ID ' || p_id_gara_esterna || ': ' || SQLERRM);
        END IF;
        RETURN 0;
END FN_INCENTIVO_MASAF_GARA_FISE;


   FUNCTION GET_DISCIPLINA (p_gara_id IN NUMBER)
      RETURN NUMBER
   IS
      v_disciplina   NUMBER := 0;
   BEGIN
      SELECT td.SEQU_ID_TIPO_DISCIPLINA
        INTO v_disciplina
        FROM TC_DATI_GARA_ESTERNA dg
             JOIN
             TC_DATI_EDIZIONE_ESTERNA ee
                ON ee.sequ_id_dati_edizione_esterna =
                      DG.FK_SEQU_ID_DATI_EDIZ_ESTERNA
             JOIN TC_EDIZIONE ed
                ON ed.sequ_id_edizione = EE.FK_SEQU_ID_EDIZIONE
             JOIN TC_MANIFESTAZIONE mf
                ON mf.sequ_id_manifestazione = ed.fk_sequ_id_manifestazione
             JOIN TD_TIPOLOGIA_DISCIPLINA td
                ON td.sequ_id_tipo_disciplina = mf.fk_sequ_id_tipo_disciplina
       WHERE dg.sequ_id_dati_gara_esterna = p_gara_id;

      RETURN v_disciplina;
   END;


   FUNCTION FN_INFO_GARA_ESTERNA (p_gara_id IN NUMBER)
      RETURN tc_dati_gara_esterna%ROWTYPE
   IS
      v_dati_gara            tc_dati_gara_esterna%ROWTYPE;
      v_disciplina           NUMBER;
      v_desc_nome_manifest   VARCHAR2 (250);

      CURSOR c_mappa (p_tipologia VARCHAR2)
      IS
         SELECT DESCRIZIONE, FK_SEQU_ID_TIPO_DISCIPLINA
           FROM TD_MANIFESTAZIONE_TIPOLOGICHE
          WHERE tipologia = p_tipologia;

   BEGIN
      --QUI NON CI POSSONO ESSERE DML perchè è chiamata da una funzione che restituisce una table function

      -- Recupero riga dalla tabella gara_esterna
      SELECT *
        INTO v_dati_gara
        FROM tc_dati_gara_esterna
       WHERE sequ_id_dati_gara_esterna = p_gara_id;


      -- Recupero riga dalla tabella gara_esterna
      SELECT mf.desc_denom_manifestazione
        INTO v_desc_nome_manifest
        FROM TC_DATI_GARA_ESTERNA dg
             JOIN
             TC_DATI_EDIZIONE_ESTERNA ee
                ON ee.sequ_id_dati_edizione_esterna =
                      DG.FK_SEQU_ID_DATI_EDIZ_ESTERNA
             JOIN TC_EDIZIONE ed
                ON ed.sequ_id_edizione = EE.FK_SEQU_ID_EDIZIONE
             JOIN TC_MANIFESTAZIONE mf
                ON mf.sequ_id_manifestazione = ed.fk_sequ_id_manifestazione
       WHERE dg.SEQU_ID_DATI_GARA_ESTERNA = p_gara_id;

      -- recupero la disciplina della gara
      v_disciplina := GET_DISCIPLINA (p_gara_id);

      -- Deduzioni intelligenti sui campi principali

      --SALTO AD OSTACOLI
      IF (v_disciplina = 4)
      THEN
         -- se manca un valore FK cerco di desumerlo dal nome della gara o da altri campi

         IF v_dati_gara.fk_codi_categoria IS NULL
         THEN
            v_dati_gara.fk_codi_categoria :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'ALTO') > 0                            --LIVELLO
                  THEN
                     74
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'GIUDIZIO') > 0
                  THEN
                     73                                              --'Elite'
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'ELITE') > 0
                  THEN
                     73                                              --'Elite'
                  WHEN v_dati_gara.DESC_ALTEZZA_OSTACOLI BETWEEN 125 AND 130
                  THEN
                     74                                               --'Alto'
                  WHEN v_dati_gara.DESC_ALTEZZA_OSTACOLI BETWEEN 110 AND 120
                  THEN
                     75                                          --'Selezione'
                  WHEN v_dati_gara.desc_codice_categoria LIKE 'CAT.E'
                  THEN
                     73                                               -- ELITE
                  ELSE
                     66                                              --'Sport'
               END;
         --UPDATE tc_dati_gara_esterna
         --   SET fk_codi_categoria = v_dati_gara.fk_codi_categoria
         --WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;

         IF v_dati_gara.fk_codi_tipo_classifica IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_classifica :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'GIUDIZIO') > 0
                  THEN
                     58                                           --'GIUDIZIO'
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'TEMPO') > 0
                  THEN
                     2                                               --'TEMPO'
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'BARRAGE') > 0
                  THEN
                     76                                            --'BARRAGE'
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'ADDESTRATIVA') > 0
                  THEN
                     77                                       --'ADDESTRATIVA'
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'COMBINATA') > 0
                  THEN
                     3                                           --'COMBINATA'
                  ELSE
                     NULL
               END;
         --    UPDATE tc_dati_gara_esterna
         --     SET fk_codi_tipo_classifica =
         --             v_dati_gara.fk_codi_tipo_classifica
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;


         IF v_dati_gara.fk_codi_eta IS NULL
         THEN
            v_dati_gara.fk_codi_eta :=
               CASE
                  WHEN INSTR (v_dati_gara.desc_nome_gara_esterna, '4 ANNI') >
                          0
                  THEN
                     59                                                    --4
                  WHEN INSTR (v_dati_gara.desc_nome_gara_esterna, '5 ANNI') >
                          0
                  THEN
                     60
                  WHEN INSTR (v_dati_gara.desc_nome_gara_esterna, '6 ANNI') >
                          0
                  THEN
                     61
                  WHEN INSTR (v_dati_gara.desc_nome_gara_esterna, '7 ANNI') >
                          0
                  THEN
                     62
                  WHEN INSTR (v_dati_gara.desc_nome_gara_esterna, '8 ANNI') >
                          0
                  THEN
                     63
                  ELSE
                     NULL
               END;
         --   UPDATE tc_dati_gara_esterna
         --     SET fk_codi_eta = v_dati_gara.fk_codi_eta
         --  WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;


         IF v_dati_gara.fk_codi_tipo_evento IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_evento :=
               CASE
                  WHEN    INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'FINALE') > 0
                       OR INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'CAMPIONATO') > 0
                       OR INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'CRITERIUM') > 0
                  THEN
                     106                                           -- 'FINALE'
                  WHEN    INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'TAPPA') > 0
                       OR INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'QUALIFICA') > 0
                       OR INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'CIRCUITO') > 0
                  THEN
                     65                                             -- 'TAPPA'
                  ELSE
                     NULL
               END;
         --  UPDATE tc_dati_gara_esterna
         --     SET fk_codi_tipo_evento = v_dati_gara.fk_codi_tipo_evento
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;
      END IF;


      --ENDURANCE
      IF (v_disciplina = 2)
      THEN
         IF v_dati_gara.fk_codi_categoria IS NULL
         THEN
            v_dati_gara.fk_codi_categoria :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEI1*') > 0
                  THEN
                     23
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEIYJ1*') > 0
                  THEN
                     24
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEI2*') > 0
                  THEN
                     25
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEIYJ2*') > 0
                  THEN
                     26
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEI3*') > 0
                  THEN
                     27
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEN B') > 0
                  THEN
                     28
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEN A') > 0
                  THEN
                     29
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'DEBUTTANTI') > 0
                  THEN
                     30
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'PROMOZIONALI') > 0
                  THEN
                     31
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CEN B/R') > 0
                  THEN
                     102
                  ELSE
                     NULL
               END;
         --   UPDATE tc_dati_gara_esterna
         --      SET fk_codi_categoria = v_dati_gara.fk_codi_categoria
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;



         IF v_dati_gara.fk_codi_tipo_evento IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_evento :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'FINALE') > 0
                  THEN
                     103
                  ELSE
                     102                                               --TAPPA
               END;
         --  UPDATE tc_dati_gara_esterna
         --     SET fk_codi_tipo_evento = v_dati_gara.fk_codi_tipo_evento
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;
      END IF;

      --DRESSAGE
      IF (v_disciplina = 6)
      THEN
         IF v_dati_gara.fk_codi_tipo_evento IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_evento :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'FINALE') > 0
                  THEN
                     5
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CAMPIONATO') > 0
                  THEN
                     6
                  ELSE
                     4
               END;
         --   UPDATE tc_dati_gara_esterna
         --      SET fk_codi_tipo_evento = v_dati_gara.fk_codi_tipo_evento
         --    WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;

         IF v_dati_gara.fk_codi_livello_cavallo IS NULL
         THEN
            v_dati_gara.fk_codi_livello_cavallo :=
               CASE
                  WHEN v_dati_gara.codi_prontuario LIKE 'E%' THEN 64
                  WHEN v_dati_gara.codi_prontuario LIKE 'F%' THEN 67
                  WHEN v_dati_gara.codi_prontuario LIKE 'M%' THEN 68
                  WHEN v_dati_gara.codi_prontuario LIKE 'D%' THEN 69
                  WHEN v_dati_gara.codi_prontuario LIKE 'C%' THEN 70
                  WHEN v_dati_gara.codi_prontuario LIKE 'B%' THEN 71
                  WHEN v_dati_gara.codi_prontuario LIKE 'A%' THEN 72
                  ELSE 64                                     -- fallback base
               END;
         -- UPDATE tc_dati_gara_esterna
         --   SET fk_codi_livello_cavallo =
         --          v_dati_gara.fk_codi_livello_cavallo
         -- WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;
      END IF;


      --ALLEVATORIALE
      IF (v_disciplina = 1)
      THEN
         IF v_dati_gara.fk_codi_tipo_prova IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_prova :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'SALTO IN LIBERTA') > 0
                  THEN
                     10                                 -- 'salto in libertà';
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'MORFO') > 0
                  THEN
                     9
                  --'morfo-attitudinale';
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'OBBEDIENZA') > 0
                  THEN
                     11
                  --'obbedienza ed andature';
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'ATTITUDINE AL SALTO') > 0
                  THEN
                     12
                  --  'attitudine al salto';
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'RASSEGNA') > 0
                  THEN
                     13
                  -- 'rassegna foals';
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CLASSIFICA COMBINATA') > 0
                  THEN
                     14
                  --  'classifica combinata';
                  ELSE
                     NULL                                 --'tipo sconosciuto'
               END;
         --   UPDATE tc_dati_gara_esterna
         -- -     SET fk_codi_tipo_prova = v_dati_gara.fk_codi_tipo_prova
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;


         IF v_dati_gara.fk_codi_tipo_evento IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_evento :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'FINALE') > 0
                  THEN
                     22
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'TAPPA') > 0
                  THEN
                     19
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'INTERREGIONALE') > 0
                  THEN
                     21
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'REGIONALE') > 0
                  THEN
                     20
                  ELSE
                     NULL
               END;

            IF     v_dati_gara.fk_codi_tipo_evento IS NULL
               AND INSTR (UPPER (v_desc_nome_manifest), 'TAPPE') > 0
            THEN
               v_dati_gara.fk_codi_tipo_evento := 19;
            END IF;
         --  UPDATE tc_dati_gara_esterna
         --     SET fk_codi_tipo_evento = v_dati_gara.fk_codi_tipo_evento
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;

         IF v_dati_gara.fk_codi_categoria IS NULL
         THEN
            v_dati_gara.fk_codi_categoria :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '1 ANNO') > 0
                  THEN
                     89
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '2 ANNI') > 0
                  THEN
                     90
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '3 ANNI') > 0
                  THEN
                     91
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'FOAL') > 0
                  THEN
                     88
                  ELSE
                     NULL
               END;
         --  UPDATE tc_dati_gara_esterna
         --     SET fk_codi_categoria = v_dati_gara.fk_codi_categoria
         --   WHERE sequ_id_dati_gara_esterna = p_gara_id;
         END IF;
      END IF;

      --COMPLETO
      IF (v_disciplina = 3)
      THEN
         -- 1. DEDUZIONE FK_CODI_CATEGORIA (Livello/Tecnica della gara: CN80, CN90, CCI2*-S, etc.)
         -- Per il Completo, la "categoria" spesso si riferisce al livello tecnico della gara.
         IF v_dati_gara.fk_codi_livello_cavallo IS NULL
         THEN
            v_dati_gara.fk_codi_livello_cavallo :=
               CASE
                  -- Livelli Internazionali (più specifici)
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CCN**') > 0
                  THEN
                     50
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CCI2*-S') > 0
                  THEN
                     51
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CCIYH1*') > 0
                  THEN
                     52
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CCI3*') > 0
                  THEN
                     53
                  -- Livelli Nazionali per Giovani Cavalli MASAF o Trofeo
                  WHEN    INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'CN1*') > 0
                       OR INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                                 'CN105') > 0
                  THEN
                     48                                   -- Es. 43 per 5 anni
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CN2*') > 0
                  THEN
                     49                             -- Es. 46 per Camp. 6 anni
                  -- Potrebbe essere necessario un default o NULL se non identificabile
                  ELSE
                     NULL
               END;
         --  IF v_dati_gara.fk_codi_livello_cavallo IS NOT NULL
         --  THEN                 -- Aggiorna solo se un valore è stato dedotto
         --     UPDATE tc_dati_gara_esterna
         --        SET fk_codi_livello_cavallo =
         --               v_dati_gara.fk_codi_livello_cavallo
         --      WHERE sequ_id_dati_gara_esterna = p_gara_id;
         --  END IF;
         END IF;

         -- 2. DEDUZIONE fk_codi_categoria (Età del cavallo, se specificata o parte del nome)
         -- Questo è cruciale per il Circuito Giovani Cavalli e i Campionati MASAF
         IF v_dati_gara.fk_codi_categoria IS NULL
         THEN
            v_dati_gara.fk_codi_categoria :=
               CASE
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '7 ANNI') > 0
                  THEN
                     100                              -- Es. 50 (Camp. 7 anni)
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '6 ANNI') > 0
                  THEN
                     99                               -- Es. 49 (Camp. 6 anni)
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '5 ANNI') > 0
                  THEN
                     98                          -- Es. 48 (Circuito 4/5 anni)
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              '4 ANNI') > 0
                  THEN
                     97                          -- Es. 47 (Circuito 4/5 anni)
                  ELSE
                     NULL -- L'età potrebbe non essere sempre nel nome per gare open
               END;
         --  IF v_dati_gara.fk_codi_categoria IS NOT NULL
         --  THEN
         --     UPDATE tc_dati_gara_esterna
         --        SET fk_codi_categoria = v_dati_gara.fk_codi_categoria
         --      WHERE sequ_id_dati_gara_esterna = p_gara_id;
         --  END IF;
         END IF;

         -- 3. DEDUZIONE FK_CODI_TIPO_EVENTO (Tappa, Finale, Campionato)
         IF v_dati_gara.fk_codi_tipo_evento IS NULL
         THEN
            v_dati_gara.fk_codi_tipo_evento :=
               CASE
                  -- "CAMPIONATO MASAF" o "CAMPIONATO DEL MONDO" sono più specifici
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'CAMPIONATO MASAF') > 0
                  THEN
                     55                                              -- Es. 51
                  WHEN INSTR (UPPER (v_dati_gara.desc_nome_gara_esterna),
                              'TAPPA') > 0
                  THEN
                     54                         -- Es. 38 (Tappa Circuito 4/5)
                  -- Se il nome dell'edizione (non della gara) contiene "CIRCUITO MASAF" e non è una finale/campionato, potrebbe essere una tappa.
                  -- Questa logica richiederebbe di accedere al nome dell'edizione.
                  -- Per ora, se nessuna keyword esplicita è nel nome gara:
                  ELSE
                     NULL -- Per il completo, è più difficile assumere un default "TAPPA" senza keyword.
               END;
         --  IF v_dati_gara.fk_codi_tipo_evento IS NOT NULL
         --  THEN
         ---     UPDATE tc_dati_gara_esterna
         --        SET fk_codi_tipo_evento = v_dati_gara.fk_codi_tipo_evento
         --      WHERE sequ_id_dati_gara_esterna = p_gara_id;
         -- END IF;
         END IF;
      END IF;


      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE (
            'FN_INFO_GARA_ESTERNA v_disciplina ' || v_disciplina);
         DBMS_OUTPUT.PUT_LINE (
            ' - Nome gara :' || v_dati_gara.desc_nome_gara_esterna);
         DBMS_OUTPUT.PUT_LINE (
               ' sequ_id_dati_gara_esterna='
            || v_dati_gara.sequ_id_dati_gara_esterna);
         DBMS_OUTPUT.PUT_LINE (
               '   tipo classifica    '
            || v_dati_gara.fk_codi_tipo_classifica
            || ' '
            || UPPER (
                  FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_tipo_classifica)));
         DBMS_OUTPUT.PUT_LINE (
               '   categoria '
            || v_dati_gara.fk_codi_categoria
            || ' '
            || UPPER (FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_categoria)));
         DBMS_OUTPUT.PUT_LINE (
               '    tipo prova '
            || v_dati_gara.fk_codi_tipo_prova
            || ' '
            || UPPER (FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_tipo_prova)));
         DBMS_OUTPUT.PUT_LINE (
               '    tipo evento '
            || v_dati_gara.fk_codi_tipo_evento
            || ' '
            || UPPER (FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_tipo_evento)));
         DBMS_OUTPUT.PUT_LINE (
               '    eta '
            || v_dati_gara.fk_codi_eta
            || ' '
            || UPPER (FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_eta)));
         DBMS_OUTPUT.PUT_LINE (
               '    livello cavallo '
            || v_dati_gara.fk_codi_livello_cavallo
            || ' '
            || UPPER (
                  FN_DESC_TIPOLOGICA (v_dati_gara.fk_codi_livello_cavallo)));
      END IF;

      RETURN v_dati_gara;
   END FN_INFO_GARA_ESTERNA;

   FUNCTION FN_INFO_GARA_SQL (p_gara_id IN NUMBER)
      RETURN tc_dati_gara_esterna_tbl
      PIPELINED
   IS
      l_riga   tc_dati_gara_esterna_obj;
      r        tc_dati_gara_esterna%ROWTYPE;
   BEGIN
      -- Recupero dati gara
      r := FN_INFO_GARA_ESTERNA (p_gara_id);



      l_riga.fk_sequ_id_dati_ediz_esterna := r.fk_sequ_id_dati_ediz_esterna;
      l_riga.sequ_id_dati_gara_esterna := r.sequ_id_dati_gara_esterna;
      l_riga.fk_sequ_id_gara_manifestazioni :=
         r.fk_sequ_id_gara_manifestazioni;
      l_riga.codi_gara_esterna := r.codi_gara_esterna;
      l_riga.desc_nome_gara_esterna := r.desc_nome_gara_esterna;
      l_riga.data_gara_esterna := r.data_gara_esterna;
      l_riga.desc_gruppo_categoria := r.desc_gruppo_categoria;
      l_riga.desc_codice_categoria := r.desc_codice_categoria;
      l_riga.desc_altezza_ostacoli := r.desc_altezza_ostacoli;
      l_riga.flag_gran_premio := r.flag_gran_premio;
      l_riga.codi_utente_inserimento := r.codi_utente_inserimento;
      l_riga.dttm_inserimento := r.dttm_inserimento;
      l_riga.codi_utente_aggiornamento := r.codi_utente_aggiornamento;
      l_riga.dttm_aggiornamento := r.dttm_aggiornamento;
      l_riga.flag_prova_a_squadre := r.flag_prova_a_squadre;
      l_riga.nume_mance := r.nume_mance;
      l_riga.codi_prontuario := r.codi_prontuario;
      l_riga.nume_cavalli_italiani := r.nume_cavalli_italiani;
      l_riga.desc_formula := r.desc_formula;
      l_riga.data_dressage := r.data_dressage;
      l_riga.data_cross := r.data_cross;
      l_riga.fk_codi_categoria := r.fk_codi_categoria;
      l_riga.fk_codi_tipo_classifica := r.fk_codi_tipo_classifica;
      l_riga.fk_codi_livello_cavallo := r.fk_codi_livello_cavallo;
      l_riga.fk_codi_tipo_evento := r.fk_codi_tipo_evento;
      l_riga.fk_codi_tipo_prova := r.fk_codi_tipo_prova;
      l_riga.fk_codi_regola_sesso := r.fk_codi_regola_sesso;
      l_riga.fk_codi_regola_libro := r.fk_codi_regola_libro;
      l_riga.fk_codi_eta := r.fk_codi_eta;
      l_riga.flag_premio_masaf := r.flag_premio_masaf;
      PIPE ROW (l_riga);


      RETURN;
   END FN_INFO_GARA_SQL;


   FUNCTION FN_PERIODO_SALTO_OSTACOLI (p_data_gara VARCHAR2 -- formato: 'YYYYMMDD'
                                                           )
      RETURN NUMBER
   IS
      v_data             DATE;
      v_anno             NUMBER;
      v_inizio_primo     DATE;
      v_fine_primo       DATE;
      v_inizio_secondo   DATE;
      v_fine_secondo     DATE;
   BEGIN
      -- Conversione stringa -> data
      v_data := TO_DATE (p_data_gara, 'YYYYMMDD');
      v_anno := TO_NUMBER (TO_CHAR (v_data, 'YYYY'));

      -- Calcolo date primo periodo (terza domenica marzo -> ultima domenica maggio)
      v_inizio_primo :=
           NEXT_DAY (TO_DATE ('01-03-' || v_anno, 'DD-MM-YYYY') - 1,
                     'DOMENICA')
         + 14;
      v_fine_primo :=
         NEXT_DAY (LAST_DAY (TO_DATE ('01-05-' || v_anno, 'DD-MM-YYYY')) - 7,
                   'DOMENICA');

      -- Calcolo date secondo periodo (prima -> seconda domenica settembre)
      v_inizio_secondo :=
         NEXT_DAY (TO_DATE ('01-06-' || v_anno, 'DD-MM-YYYY') - 1,
                   'DOMENICA');
      v_fine_secondo :=
         NEXT_DAY (
            NEXT_DAY (TO_DATE ('01-09-' || v_anno, 'DD-MM-YYYY') - 1,
                      'DOMENICA'),
            'DOMENICA');

      -- Determinazione periodo
      IF v_data BETWEEN v_inizio_primo AND v_fine_primo
      THEN
         RETURN 1;
      ELSIF v_data BETWEEN v_inizio_secondo AND v_fine_secondo
      THEN
         RETURN 2;
      ELSE
         RETURN 0;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN -1;
   END FN_PERIODO_SALTO_OSTACOLI;


   FUNCTION FN_CALCOLO_SALTO_OSTACOLI_SIM (p_gara_id        IN NUMBER,
                                           p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      l_risultati              t_tabella_premi;
      v_rec                    UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST.t_premio_rec;
      v_disciplina             VARCHAR2 (50);
      v_categoria              VARCHAR2 (50);
      v_eta                    NUMBER;
      v_anno_nascita_cavallo   NUMBER;
      v_periodo                NUMBER;
      v_data_gara              VARCHAR2 (8);
      v_premio                 NUMBER;
      v_dati_gara              tc_dati_gara_esterna%ROWTYPE;
   BEGIN
      l_risultati := t_tabella_premi ();

      -- Recupero dati gara
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);

      -- Calcolo periodo campo dedotto dalla data della gara
      v_periodo := FN_PERIODO_SALTO_OSTACOLI (v_dati_gara.data_gara_esterna);


      -- Categoria
      SELECT MIN (anno_nascita_cavallo)
        INTO v_anno_nascita_cavallo
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      FOR i IN 1 .. p_num_partenti
      LOOP
         v_rec.cavallo_id := 1000 + i;
         v_rec.nome_cavallo := 'Cavallo ' || CHR (64 + i);
         v_rec.posizione := i;



         CALCOLA_PREMIO_SALTO_OST_2025 (
            v_dati_gara,
            p_periodo                      => v_periodo,
            p_num_partenti                 => p_num_partenti,
            p_posizione                    => i,
            p_montepremi_tot               => NULL,
            p_num_con_parimerito           => 1,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => NULL,
            p_premio_cavallo               => v_premio);
         v_rec.premio := v_premio;
         v_rec.note :=
               'v_disciplina:'
            || 'Salto ad Ostacoli'
            || ',v_num_partenti:'
            || p_num_partenti
            || ',v_categoria:'
            || v_categoria
            || ',v_eta:'
            || v_eta;


         l_risultati.EXTEND;
         l_risultati (l_risultati.COUNT) := v_rec;
      END LOOP;

      FOR i IN 1 .. l_risultati.COUNT
      LOOP
         PIPE ROW (l_risultati (i));
      END LOOP;

      RETURN;
   END FN_CALCOLO_SALTO_OSTACOLI_SIM;

   FUNCTION FN_CALCOLO_DRESSAGE_SIM (p_gara_id        IN NUMBER,
                                     p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      l_riga                   t_premio_rec;
      v_posizione              NUMBER;
      v_punteggio              NUMBER;
      v_premio                 NUMBER;
      v_parimerito             NUMBER;
      v_montepremi             NUMBER;
      v_eta                    NUMBER;
      v_data_gara              VARCHAR2 (8);
      v_anno_nascita_cavallo   NUMBER;
      v_periodo                NUMBER;
      v_dati_gara              tc_dati_gara_esterna%ROWTYPE;
   BEGIN
      -- Recupero dati gara
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);

      -- Calcolo periodo
      v_periodo := FN_PERIODO_SALTO_OSTACOLI (v_dati_gara.data_gara_esterna);

      -- Calcolo montepremi reale
      v_montepremi := 10000;


      SELECT MIN (anno_nascita_cavallo)
        INTO v_anno_nascita_cavallo
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      v_eta :=
           TO_NUMBER (SUBSTR (v_data_gara, 1, 4))
         - NVL (v_anno_nascita_cavallo,
                TO_NUMBER (SUBSTR (v_data_gara, 1, 4)));


      FOR v_posizione IN 1 .. p_num_partenti
      LOOP
         v_punteggio := 65 + (5 - v_posizione);

         IF MOD (v_posizione, 2) = 0
         THEN
            v_parimerito := 2;
         ELSE
            v_parimerito := 1;
         END IF;

         CALCOLA_PREMIO_DRESSAGE_2025 (
            p_dati_gara                    => v_dati_gara,
            p_tipo_evento                  => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_TIPO_EVENTO),
            p_flag_fise                    => 0,
            p_livello_cavallo              => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_LIVELLO_CAVALLO),
            p_posizione                    => v_posizione,
            p_punteggio                    => v_punteggio,
            p_montepremi_tot               => NULL,
            p_num_con_parimerito           => v_parimerito,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => NULL,
            p_numero_giornata              => v_periodo,
            p_premio_cavallo               => v_premio);


         l_riga.cavallo_id := v_posizione;
         l_riga.nome_cavallo := 'Cavallo ' || CHR (64 + v_posizione);
         l_riga.premio := v_premio;
         l_riga.posizione := v_posizione;
         l_riga.note := 'Simulazione posizione ' || v_posizione;

         PIPE ROW (l_riga);
      END LOOP;

      RETURN;
   END FN_CALCOLO_DRESSAGE_SIM;


   FUNCTION FN_CALCOLO_ENDURANCE_SIM (p_gara_id        IN NUMBER,
                                      p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      l_risultati      t_tabella_premi;
      v_rec            UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST.t_premio_rec;
      v_categoria      VARCHAR2 (50);
      v_montepremi     NUMBER := 0;
      v_num_partenti   NUMBER;
      v_premio         NUMBER;
      v_tipo_evento    VARCHAR2 (50);
      v_tipo_prova     VARCHAR2 (50);
      v_dati_gara      tc_dati_gara_esterna%ROWTYPE;
   BEGIN
      l_risultati := t_tabella_premi ();

      -- Recupero dati gara
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);

      -- Determina il montepremi in base a categoria e tipo evento
      v_montepremi :=
         CASE UPPER (FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA))
            WHEN 'DEBUTTANTI' THEN 2700
            WHEN 'CEN A' THEN 3600
            WHEN 'CEN B/R' THEN 4200
            ELSE -1
         END;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE (
               'FN_CALCOLO_ENDURANCE_SIM v_montepremi cat.'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA)
            || ' '
            || v_montepremi);
      END IF;

      -- Simulazione premi: distribuiamo ai primi 6, se ci sono abbastanza partenti
      FOR i IN 1 .. p_num_partenti
      LOOP
         v_rec.cavallo_id := 1000 + i;
         v_rec.nome_cavallo := 'Cavallo ' || CHR (64 + i);
         v_rec.posizione := i;

         CALCOLA_PREMIO_ENDURANCE_2025 (
            p_dati_gara                    => v_dati_gara,
            p_categoria                    => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_CATEGORIA),
            p_tipo_evento                  => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_TIPO_EVENTO),
            p_flag_fise                    => 0,
            p_posizione                    => i,
            p_montepremi_tot               => v_montepremi,
            p_num_con_parimerito           => 1,
            p_premio_cavallo               => v_premio,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => NULL);



         v_rec.premio := v_premio;
         v_rec.note :=
               'v_disciplina:'
            || 'Endurance '
            || ',v_num_partenti:'
            || p_num_partenti
            || ',v_categoria:'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA)
            || ',v_tipo_evento:'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_TIPO_EVENTO)
            || ',v_tipo_prova:'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_TIPO_PROVA);


         l_risultati.EXTEND;
         l_risultati (l_risultati.COUNT) := v_rec;
      END LOOP;

      FOR i IN 1 .. l_risultati.COUNT
      LOOP
         PIPE ROW (l_risultati (i));
      END LOOP;

      RETURN;
   END;


   FUNCTION FN_CALCOLO_ALLEVATORIALE_SIM (p_gara_id        IN NUMBER,
                                          p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      v_dati_gara   tc_dati_gara_esterna%ROWTYPE;
      l_risultati   t_tabella_premi;
      v_rec         UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST.t_premio_rec;
      v_premio      NUMBER;
   BEGIN
      l_risultati := t_tabella_premi ();

      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);



      FOR i IN 1 .. p_num_partenti
      LOOP
         v_rec.cavallo_id := 1000 + i;
         v_rec.nome_cavallo := 'Cavallo ' || CHR (64 + i);
         v_rec.posizione := i;


         CALCOLA_PREMIO_ALLEV_2025 (p_dati_gara                    => v_dati_gara,
                                    p_posizione                    => i,
                                    p_tot_partenti                 => p_num_partenti,
                                    p_num_con_parimerito           => 1,
                                    p_premio_cavallo               => v_premio,
                                    p_SEQU_ID_CLASSIFICA_ESTERNA   => NULL);

         v_rec.premio := v_premio;
         v_rec.note :=
               'v_disciplina:'
            || 'Allevatoriale '
            || ',v_num_partenti:'
            || p_num_partenti;

         l_risultati.EXTEND;
         l_risultati (l_risultati.COUNT) := v_rec;
      END LOOP;

      FOR i IN 1 .. l_risultati.COUNT
      LOOP
         PIPE ROW (l_risultati (i));
      END LOOP;

      RETURN;
   END FN_CALCOLO_ALLEVATORIALE_SIM;

   FUNCTION FN_CALCOLO_COMPLETO_SIM (p_gara_id        IN NUMBER,
                                     p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      v_dati_gara   tc_dati_gara_esterna%ROWTYPE;
      l_risultati   t_tabella_premi;
      v_rec         UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST.t_premio_rec;
      v_premio      NUMBER;
   BEGIN
      l_risultati := t_tabella_premi ();

      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);



      FOR i IN 1 .. p_num_partenti
      LOOP
         v_rec.cavallo_id := 1000 + i;
         v_rec.nome_cavallo := 'Cavallo ' || CHR (64 + i);
         v_rec.posizione := i;


         CALCOLA_PREMIO_COMPLETO_2025 (p_dati_gara                    => v_dati_gara,
                                       p_posizione                    => i,
                                       p_tot_partenti                 => p_num_partenti,
                                       p_num_con_parimerito           => 1,
                                       p_premio_cavallo               => v_premio,
                                       p_SEQU_ID_CLASSIFICA_ESTERNA   => NULL);

         v_rec.premio := v_premio;
         v_rec.note :=
               'v_disciplina:'
            || 'Allevatoriale '
            || ',v_num_partenti:'
            || p_num_partenti;

         l_risultati.EXTEND;
         l_risultati (l_risultati.COUNT) := v_rec;
      END LOOP;

      FOR i IN 1 .. l_risultati.COUNT
      LOOP
         PIPE ROW (l_risultati (i));
      END LOOP;

      RETURN;
   END FN_CALCOLO_COMPLETO_SIM;

   FUNCTION FN_CALCOLO_MONTA_DA_LAVORO_SIM (p_gara_id        IN NUMBER,
                                            p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED
   IS
      v_dati_gara   tc_dati_gara_esterna%ROWTYPE;
      l_risultati   t_tabella_premi;
      v_rec         UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST.t_premio_rec;
      v_premio      NUMBER;
   BEGIN
      l_risultati := t_tabella_premi ();

      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);


      FOR i IN 1 .. p_num_partenti
      LOOP
         v_rec.cavallo_id := 1000 + i;
         v_rec.nome_cavallo := 'Cavallo ' || CHR (64 + i);
         v_rec.posizione := i;


         CALCOLA_PREMIO_MONTA_2025 (p_dati_gara                    => v_dati_gara,
                                    p_posizione                    => i,
                                    p_tot_partenti                 => p_num_partenti,
                                    p_num_con_parimerito           => 1,
                                    p_premio_cavallo               => v_premio,
                                    p_SEQU_ID_CLASSIFICA_ESTERNA   => NULL);

         v_rec.premio := v_premio;
         v_rec.note :=
               'v_disciplina:'
            || 'Allevatoriale '
            || ',v_num_partenti:'
            || p_num_partenti;

         l_risultati.EXTEND;
         l_risultati (l_risultati.COUNT) := v_rec;
      END LOOP;

      FOR i IN 1 .. l_risultati.COUNT
      LOOP
         PIPE ROW (l_risultati (i));
      END LOOP;

      RETURN;
   END FN_CALCOLO_MONTA_DA_LAVORO_SIM;



   -- HANDLER SALTO AD OSTACOLI
   FUNCTION HANDLER_SALTO_OSTACOLI (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
   IS
      l_risultati        t_tabella_premi := t_tabella_premi ();
      i                  PLS_INTEGER := 0;

      v_categoria        VARCHAR2 (50);
      v_periodo          NUMBER;
      v_num_partenti     NUMBER;
      v_data_gara        VARCHAR2 (8);
      v_eta              NUMBER;
      v_premio           NUMBER;
      v_montepremi_eff   NUMBER := 0;
      v_dati_gara        tc_dati_gara_esterna%ROWTYPE;

      TYPE t_mappa_parimerito IS TABLE OF PLS_INTEGER
         INDEX BY PLS_INTEGER;

      v_parimerito       t_mappa_parimerito;

      CURSOR c_classifica
      IS
           SELECT *
             FROM tc_dati_classifica_esterna
            WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
         ORDER BY nume_piazzamento ASC;

   BEGIN
      -- Recupero dati gara
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);

      -- Calcolo periodo
      v_periodo := FN_PERIODO_SALTO_OSTACOLI (v_dati_gara.data_gara_esterna);

      -- Numero partenti
      SELECT COUNT (*)
        INTO v_num_partenti
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      IF v_num_partenti = 0
      THEN
         RETURN l_risultati;
      END IF;

      -- Mappa parimerito
      FOR rec IN (  SELECT nume_piazzamento, COUNT (*) conta
                      FROM tc_dati_classifica_esterna
                     WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
                  GROUP BY nume_piazzamento)
      LOOP
         v_parimerito (rec.nume_piazzamento) := rec.conta;
      END LOOP;



      -- Ciclo sui classificati
      FOR rec IN c_classifica
      LOOP
         CALCOLA_PREMIO_SALTO_OST_2025 (
            v_dati_gara,
            p_periodo                      => v_periodo,
            p_num_partenti                 => v_num_partenti,
            p_posizione                    => rec.nume_piazzamento,
            p_montepremi_tot               => NULL,
            p_num_con_parimerito           => NVL (
                                                v_parimerito (rec.nume_piazzamento),
                                                1),
            p_premio_cavallo               => v_premio,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => rec.SEQU_ID_CLASSIFICA_ESTERNA);

         v_montepremi_eff := v_montepremi_eff + NVL (v_premio, 0);

         i := i + 1;
         l_risultati.EXTEND;
         l_risultati (i).cavallo_id := rec.fk_sequ_id_cavallo;
         l_risultati (i).nome_cavallo := rec.desc_cavallo;
         l_risultati (i).premio := v_premio;
         l_risultati (i).posizione := rec.nume_piazzamento;
         l_risultati (i).note :=
               'v_disciplina:Salto ad ostacoli'
            || ',v_num_partenti:'
            || v_num_partenti
            || ',v_categoria:'
            || v_categoria
            || ',v_eta:'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA)
            || ',v_montepremi_eff:'
            || v_montepremi_eff;


         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = v_premio
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = rec.SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END LOOP;

      RETURN l_risultati;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.PUT_LINE ('ERRORE HANDLER_SALTO_OSTACOLI: ' || SQLERRM);
         RAISE;
   END;



   -- HANDLER DRESSAGE
   FUNCTION HANDLER_DRESSAGE (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
   IS
      l_risultati            t_tabella_premi := t_tabella_premi ();
      i                      PLS_INTEGER := 0;

      v_data_gara            VARCHAR2 (8);
      v_premio               NUMBER;
      v_eta                  NUMBER;
      v_montepremi_eff       NUMBER := 0;
      v_num_partenti         NUMBER;
      v_periodo              NUMBER;
      v_num_con_parimerito   NUMBER;
      v_dati_gara            tc_dati_gara_esterna%ROWTYPE;

      TYPE t_mappa_parimerito IS TABLE OF PLS_INTEGER
         INDEX BY PLS_INTEGER;

      v_parimerito           t_mappa_parimerito;

      CURSOR c_classifica
      IS
           SELECT *
             FROM tc_dati_classifica_esterna
            WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
         ORDER BY nume_piazzamento ASC;

   BEGIN
      -- Recupero dati gara
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);

      -- Calcolo periodo
      v_periodo := FN_PERIODO_SALTO_OSTACOLI (v_dati_gara.data_gara_esterna);


      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE (
            'v_nome_gara  : ' || v_dati_gara.DESC_NOME_GARA_ESTERNA);
         DBMS_OUTPUT.PUT_LINE (
               'Tipo evento trovato : '
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_TIPO_EVENTO));
      END IF;

      -- Numero partenti
      SELECT COUNT (*)
        INTO v_num_partenti
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      IF v_num_partenti = 0
      THEN
         RETURN l_risultati;
      END IF;



      -- Mappa parimerito
      FOR rec IN (  SELECT nume_piazzamento, COUNT (*) conta
                      FROM tc_dati_classifica_esterna
                     WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
                  GROUP BY nume_piazzamento)
      LOOP
         v_parimerito (rec.nume_piazzamento) := rec.conta;
      END LOOP;

      -- Ciclo premi Dressage
      FOR rec IN c_classifica
      LOOP
         v_eta :=
              TO_NUMBER (SUBSTR (v_data_gara, 1, 4))
            - NVL (rec.anno_nascita_cavallo,
                   TO_NUMBER (SUBSTR (v_data_gara, 1, 4)));


         CALCOLA_PREMIO_DRESSAGE_2025 (
            p_dati_gara                    => v_dati_gara,
            p_tipo_evento                  => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_TIPO_EVENTO),
            p_flag_fise                    => 0,
            p_livello_cavallo              => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_LIVELLO_CAVALLO),
            p_posizione                    => rec.nume_piazzamento,
            p_punteggio                    => rec.nume_punti,
            p_montepremi_tot               => NULL,
            p_num_con_parimerito           => NVL (
                                                v_parimerito (rec.nume_piazzamento),
                                                1),
            p_SEQU_ID_CLASSIFICA_ESTERNA   => rec.SEQU_ID_CLASSIFICA_ESTERNA,
            p_numero_giornata              => v_periodo,
            p_premio_cavallo               => v_premio);



         v_montepremi_eff := v_montepremi_eff + NVL (v_premio, 0);
         i := i + 1;
         l_risultati.EXTEND;
         l_risultati (i).cavallo_id := rec.fk_sequ_id_cavallo;
         l_risultati (i).nome_cavallo := rec.desc_cavallo;
         l_risultati (i).premio := v_premio;
         l_risultati (i).posizione := rec.nume_piazzamento;
         l_risultati (i).note :=
               'v_disciplina:Salto ad ostacoli'
            || ',v_num_partenti:'
            || v_num_partenti
            || ',v_eta:'
            || v_eta
            || ',v_num_con_parimerito:'
            || v_num_con_parimerito;

         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = v_premio
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = rec.SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END LOOP;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE (
            'Montepremi Totale Dressage: ' || v_montepremi_eff);
      END IF;

      RETURN l_risultati;
   END HANDLER_DRESSAGE;


   -- HANDLER ENDURANCE
   FUNCTION HANDLER_ENDURANCE (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
   IS
      l_risultati      t_tabella_premi := t_tabella_premi ();
      i                PLS_INTEGER := 0;

      v_num_partenti   NUMBER;
      v_premio         NUMBER;
      v_montepremi     NUMBER := 0;

      TYPE t_mappa_parimerito IS TABLE OF PLS_INTEGER
         INDEX BY PLS_INTEGER;

      v_parimerito     t_mappa_parimerito;
      v_dati_gara      tc_dati_gara_esterna%ROWTYPE;

      CURSOR c_classifica
      IS
           SELECT *
             FROM tc_dati_classifica_esterna
            WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
         ORDER BY nume_piazzamento ASC;

   BEGIN
      -- Recupero dati gara
      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);

      -- Determina il montepremi in base a categoria e tipo evento
      v_montepremi :=
         CASE FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA)
            WHEN 'Debuttanti' THEN 2700
            WHEN 'CEN A' THEN 3600
            WHEN 'CEN B/R' THEN 4200
            ELSE 0
         END;

      -- Mappa parimerito
      FOR rec IN (  SELECT nume_piazzamento, COUNT (*) conta
                      FROM tc_dati_classifica_esterna
                     WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
                  GROUP BY nume_piazzamento)
      LOOP
         v_parimerito (rec.nume_piazzamento) := rec.conta;
      END LOOP;


      IF v_num_partenti = 0
      THEN
         RETURN l_risultati;
      END IF;

      -- Ciclo premi Endurance
      FOR rec IN c_classifica
      LOOP
         CALCOLA_PREMIO_ENDURANCE_2025 (
            p_dati_gara                    => v_dati_gara,
            p_categoria                    => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_CATEGORIA),
            p_tipo_evento                  => FN_DESC_TIPOLOGICA (
                                                v_dati_gara.FK_CODI_TIPO_EVENTO),
            p_flag_fise                    => 0,
            p_posizione                    => rec.nume_piazzamento,
            p_montepremi_tot               => v_montepremi,
            --p_montepremi_tot       => 2700,
            p_num_con_parimerito           => NVL (
                                                v_parimerito (rec.nume_piazzamento),
                                                1),
            p_premio_cavallo               => v_premio,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => rec.SEQU_ID_CLASSIFICA_ESTERNA);


         i := i + 1;
         l_risultati.EXTEND;
         l_risultati (i).cavallo_id := rec.fk_sequ_id_cavallo;
         l_risultati (i).nome_cavallo := rec.desc_cavallo;
         l_risultati (i).premio := v_premio;
         l_risultati (i).posizione := rec.nume_piazzamento;
         l_risultati (i).note :=
               'v_disciplina:Endurance'
            || ',codi_categoria:'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_CATEGORIA)
            || ',v_tipo_prova:'
            || FN_DESC_TIPOLOGICA (v_dati_gara.FK_CODI_TIPO_PROVA)
            || ',v_montepremi:'
            || v_montepremi;

         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = v_premio
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = rec.SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END LOOP;

      RETURN l_risultati;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.PUT_LINE ('ERRORE HANDLER_ENDURANCE: ' || SQLERRM);
         RAISE;
   END;

   FUNCTION HANDLER_ALLEVATORIALE (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
   IS
      l_risultati       t_tabella_premi := t_tabella_premi ();
      i                 PLS_INTEGER := 0;

      v_dati_gara       tc_dati_gara_esterna%ROWTYPE;
      v_premio          NUMBER;
      v_count           NUMBER := 0;
      v_gara_id_altro   NUMBER;
      v_id_gara_altra   NUMBER;

      TYPE t_mappa_parimerito IS TABLE OF PLS_INTEGER
         INDEX BY PLS_INTEGER;

      v_parimerito      t_mappa_parimerito;

      CURSOR c_classifica
      IS
           SELECT *
             FROM tc_dati_classifica_esterna
            WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
         ORDER BY nume_piazzamento ASC;

   BEGIN
      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('HANDLER_ALLEVATORIALE ');
      END IF;

      ----------------------------------------------------------------------------
      -- 1) Recupero della riga di gara (tc_dati_gara_esterna) in base a p_gara_id
      ----------------------------------------------------------------------------

      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);
      l_risultati := t_tabella_premi ();


      -- Numero partenti
      SELECT COUNT (*)
        INTO v_count
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('ALLEVAT Partenti ' || v_count);
         DBMS_OUTPUT.PUT_LINE (
            'ALLEVAT Nome gara ' || v_dati_gara.desc_nome_gara_esterna);
      END IF;


      IF v_count = 0
      THEN
         RETURN l_risultati;
      END IF;

      ----------------------------------------------------------------------------
      -- Nel caso sia una gara foal devo richiamare ELABORA_PREMI_FOAL_ANNO
      ----------------------------------------------------------------------------

      IF UPPER (v_dati_gara.desc_nome_gara_esterna) LIKE '%FOAL%'
      THEN
         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE ('ELABORO UNA GARA FOAL ');
         END IF;

         -- Se almeno 4 partecipanti: elaborazione singola
         IF v_count >= 4
         THEN
            IF c_debug
            THEN
               DBMS_OUTPUT.PUT_LINE ('Gara con più di 4 partecipanti ');
            END IF;

            PKG_CALCOLI_PREMI_MANIFEST.CALCOLA_PREMIO_FOALS_2025 (
               p_id_gara_1   => p_gara_id,
               p_id_gara_2   => NULL);
         ELSE
            -- Cerca un'altra gara FOAL nella stessa edizione con < 4 partecipanti
            BEGIN
               IF c_debug
               THEN
                  DBMS_OUTPUT.PUT_LINE (
                     'Gara con meno di 4 partecipanti , cerco con chi accorpare');
               END IF;

               SELECT dg2.sequ_id_dati_gara_esterna
                 INTO v_id_gara_altra
                 FROM tc_dati_gara_esterna dg2
                WHERE     dg2.fk_sequ_id_dati_ediz_esterna =
                             v_dati_gara.FK_SEQU_ID_DATI_EDIZ_ESTERNA
                      AND dg2.sequ_id_dati_gara_esterna != p_gara_id
                      AND UPPER (dg2.desc_nome_gara_esterna) LIKE '%FOAL%'
                      AND EXISTS
                             (  SELECT 1
                                  FROM tc_dati_classifica_esterna ce
                                 WHERE ce.fk_sequ_id_dati_gara_esterna =
                                          dg2.sequ_id_dati_gara_esterna
                              GROUP BY ce.fk_sequ_id_dati_gara_esterna
                                HAVING COUNT (*) < 4)
                      AND ROWNUM = 1;


               -- Accorpamento con l¿altra gara
               PKG_CALCOLI_PREMI_MANIFEST.CALCOLA_PREMIO_FOALS_2025 (
                  p_id_gara_1   => p_gara_id,
                  p_id_gara_2   => v_id_gara_altra);
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  NULL;  -- Nessuna seconda gara disponibile ¿ non si fa nulla
            END;
         END IF;

         RETURN l_risultati;
      END IF;

      -- Mappa parimerito
      FOR rec IN (  SELECT nume_piazzamento, COUNT (*) conta
                      FROM tc_dati_classifica_esterna
                     WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
                  GROUP BY nume_piazzamento)
      LOOP
         v_parimerito (rec.nume_piazzamento) := rec.conta;
      END LOOP;



      FOR rec IN c_classifica
      LOOP
         CALCOLA_PREMIO_ALLEV_2025 (
            p_dati_gara                    => v_dati_gara,
            p_posizione                    => rec.nume_piazzamento,
            p_tot_partenti                 => v_count,
            p_num_con_parimerito           => NVL (
                                                v_parimerito (rec.nume_piazzamento),
                                                1),
            p_premio_cavallo               => v_premio,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => rec.SEQU_ID_CLASSIFICA_ESTERNA);

         i := i + 1;
         l_risultati.EXTEND;
         l_risultati (i).cavallo_id := rec.fk_sequ_id_cavallo;
         l_risultati (i).nome_cavallo := rec.desc_cavallo;
         l_risultati (i).premio := v_premio;
         l_risultati (i).posizione := rec.nume_piazzamento;
         l_risultati (i).note :=
            'v_disciplina:Allevatoriale' || ',v_premio:' || v_premio;

         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = v_premio
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = rec.SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END LOOP;

      RETURN l_risultati;
   END HANDLER_ALLEVATORIALE;

   FUNCTION HANDLER_COMPLETO (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
   IS
      l_risultati    t_tabella_premi := t_tabella_premi ();
      i              PLS_INTEGER := 0;

      v_dati_gara    tc_dati_gara_esterna%ROWTYPE;
      v_premio       NUMBER;
      v_count        NUMBER := 0;

      TYPE t_mappa_parimerito IS TABLE OF PLS_INTEGER
         INDEX BY PLS_INTEGER;

      v_parimerito   t_mappa_parimerito;

      CURSOR c_classifica
      IS
           SELECT *
             FROM tc_dati_classifica_esterna
            WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
         ORDER BY nume_piazzamento ASC;

   BEGIN
      ----------------------------------------------------------------------------
      -- 1) Recupero della riga di gara (tc_dati_gara_esterna) in base a p_gara_id
      ----------------------------------------------------------------------------

      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);
      l_risultati := t_tabella_premi ();

      ----------------------------------------------------------------------------
      -- 3) Calcolo dei premi per ciascun cavallo, invocando la procedura singola
      ----------------------------------------------------------------------------

      -- Numero partenti
      SELECT COUNT (*)
        INTO v_count
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      IF v_count = 0
      THEN
         RETURN l_risultati;
      END IF;

      -- Mappa parimerito
      FOR rec IN (  SELECT nume_piazzamento, COUNT (*) conta
                      FROM tc_dati_classifica_esterna
                     WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
                  GROUP BY nume_piazzamento)
      LOOP
         v_parimerito (rec.nume_piazzamento) := rec.conta;
      END LOOP;



      FOR rec IN c_classifica
      LOOP
         CALCOLA_PREMIO_COMPLETO_2025 (
            p_dati_gara                    => v_dati_gara,
            p_posizione                    => rec.nume_piazzamento,
            p_tot_partenti                 => v_count,
            p_num_con_parimerito           => NVL (
                                                v_parimerito (rec.nume_piazzamento),
                                                1),
            p_premio_cavallo               => v_premio,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => rec.SEQU_ID_CLASSIFICA_ESTERNA);

         i := i + 1;
         l_risultati.EXTEND;
         l_risultati (i).cavallo_id := rec.fk_sequ_id_cavallo;
         l_risultati (i).nome_cavallo := rec.desc_cavallo;
         l_risultati (i).premio := v_premio;
         l_risultati (i).posizione := rec.nume_piazzamento;
         l_risultati (i).note :=
            'v_disciplina:Completo' || ',v_premio:' || v_premio;

         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = v_premio
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = rec.SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END LOOP;

      RETURN l_risultati;
   END HANDLER_COMPLETO;

   FUNCTION HANDLER_MONTA_DA_LAVORO (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
   IS
      l_risultati    t_tabella_premi := t_tabella_premi ();
      i              PLS_INTEGER := 0;

      v_dati_gara    tc_dati_gara_esterna%ROWTYPE;
      v_premio       NUMBER;
      v_count        NUMBER := 0;

      TYPE t_mappa_parimerito IS TABLE OF PLS_INTEGER
         INDEX BY PLS_INTEGER;

      v_parimerito   t_mappa_parimerito;

      CURSOR c_classifica
      IS
           SELECT *
             FROM tc_dati_classifica_esterna
            WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
         ORDER BY nume_piazzamento ASC;

   BEGIN
      ----------------------------------------------------------------------------
      -- 1) Recupero della riga di gara (tc_dati_gara_esterna) in base a p_gara_id
      ----------------------------------------------------------------------------

      v_dati_gara := FN_INFO_GARA_ESTERNA (p_gara_id);
      l_risultati := t_tabella_premi ();

      ----------------------------------------------------------------------------
      -- 3) Calcolo dei premi per ciascun cavallo, invocando la procedura singola
      ----------------------------------------------------------------------------

      -- Numero partenti
      SELECT COUNT (*)
        INTO v_count
        FROM tc_dati_classifica_esterna
       WHERE fk_sequ_id_dati_gara_esterna = p_gara_id;

      IF v_count = 0
      THEN
         RETURN l_risultati;
      END IF;

      -- Mappa parimerito
      FOR rec IN (  SELECT nume_piazzamento, COUNT (*) conta
                      FROM tc_dati_classifica_esterna
                     WHERE fk_sequ_id_dati_gara_esterna = p_gara_id
                  GROUP BY nume_piazzamento)
      LOOP
         v_parimerito (rec.nume_piazzamento) := rec.conta;
      END LOOP;



      FOR rec IN c_classifica
      LOOP
         CALCOLA_PREMIO_MONTA_2025 (
            p_dati_gara                    => v_dati_gara,
            p_posizione                    => rec.nume_piazzamento,
            p_tot_partenti                 => v_count,
            p_num_con_parimerito           => NVL (
                                                v_parimerito (rec.nume_piazzamento),
                                                1),
            p_premio_cavallo               => v_premio,
            p_SEQU_ID_CLASSIFICA_ESTERNA   => rec.SEQU_ID_CLASSIFICA_ESTERNA);

         i := i + 1;
         l_risultati.EXTEND;
         l_risultati (i).cavallo_id := rec.fk_sequ_id_cavallo;
         l_risultati (i).nome_cavallo := rec.desc_cavallo;
         l_risultati (i).premio := v_premio;
         l_risultati (i).posizione := rec.nume_piazzamento;
         l_risultati (i).note :=
            'v_disciplina:Allevatoriale' || ',v_premio:' || v_premio;
      END LOOP;

      RETURN l_risultati;
   END HANDLER_MONTA_DA_LAVORO;

   PROCEDURE ELABORA_PREMI_GARA (p_gara_id         IN     NUMBER,
                                 p_forza_elabora   IN     NUMBER,
                                 p_risultato          OUT VARCHAR2)
   --p_risultati          OUT t_tabella_premi)
   IS
      v_disciplina            VARCHAR2 (50);
      v_premi_gia_calcolati   NUMBER (1);
      p_risultati             t_tabella_premi;
   BEGIN
      --se TRUE forzo il calcolo del montepremi

      v_disciplina := GET_DISCIPLINA (p_gara_id);

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('ELABORA_PREMI_GARA ');
      END IF;

      -- Inizializzazione collection
      p_risultati := t_tabella_premi ();


      -- Verifica se la gara ha già premi calcolati
      SELECT CASE WHEN COUNT (1) > 0 THEN 1 ELSE 0 END
        INTO v_premi_gia_calcolati
        FROM TC_DATI_CLASSIFICA_ESTERNA
       WHERE     fk_sequ_id_dati_gara_esterna = p_gara_id
             AND importo_masaf_calcolato IS NOT NULL;


      IF     FN_GARA_PREMIATA_MASAF (p_gara_id) = 1
         AND (v_premi_gia_calcolati = 0 OR p_forza_elabora = 1)
      THEN
         -- Dispatch in base alla disciplina
         CASE v_disciplina
            WHEN 1
            THEN
               p_risultati := HANDLER_ALLEVATORIALE (p_gara_id);
            WHEN 2
            THEN
               p_risultati := HANDLER_ENDURANCE (p_gara_id);
            WHEN 3
            THEN
               p_risultati := HANDLER_COMPLETO (p_gara_id);
            WHEN 4
            THEN
               p_risultati := HANDLER_SALTO_OSTACOLI (p_gara_id);
            WHEN 6
            THEN
               p_risultati := HANDLER_DRESSAGE (p_gara_id);
            WHEN 7
            THEN
               p_risultati := HANDLER_MONTA_DA_LAVORO (p_gara_id);
            ELSE
               RAISE_APPLICATION_ERROR (
                  -20001,
                  'Disciplina non gestita: ' || v_disciplina);
         END CASE;

         p_risultato := 'Elaborazione terminata correttamente.';
      ELSE
         p_risultato :=
            'Attenzione la Gara o è definita come senza premi Masaf o ha già i premi elaborati.';
      END IF;
   END;


   PROCEDURE ELABORA_PREMI_FOALS_PER_ANNO (v_anno IN VARCHAR2)
   IS
      CURSOR c_foals
      IS
         SELECT dg.sequ_id_dati_gara_esterna,
                dg.data_gara_esterna,
                ee.DESC_LOCALITA_ESTERA
           FROM tc_dati_gara_esterna dg
                JOIN
                TC_DATI_EDIZIONE_ESTERNA ee
                   ON ee.sequ_id_dati_edizione_esterna =
                         dg.FK_SEQU_ID_DATI_EDIZ_ESTERNA
          WHERE     UPPER (dg.desc_nome_gara_esterna) LIKE '%FOAL%'
                AND SUBSTR (dg.data_gara_esterna, 1, 4) = v_anno
                AND dg.fk_sequ_id_gara_manifestazioni IS NOT NULL;

      TYPE t_numlist IS TABLE OF NUMBER
         INDEX BY PLS_INTEGER;

      gare_gia_elaborate   t_numlist;

      v_count_part         NUMBER;
   BEGIN
      DBMS_OUTPUT.PUT_LINE ('--- Inizio elaborazione gare FOALS ---');

      FOR rec IN c_foals
      LOOP
         DBMS_OUTPUT.PUT_LINE (
            'Verifico gara ID: ' || rec.sequ_id_dati_gara_esterna);

         IF gare_gia_elaborate.EXISTS (rec.sequ_id_dati_gara_esterna)
         THEN
            DBMS_OUTPUT.PUT_LINE ('  -> Già elaborata, salto.');
            CONTINUE;
         END IF;

         SELECT COUNT (*)
           INTO v_count_part
           FROM tc_dati_classifica_esterna
          WHERE     fk_sequ_id_dati_gara_esterna =
                       rec.sequ_id_dati_gara_esterna
                AND nume_piazzamento IS NOT NULL;

         DBMS_OUTPUT.PUT_LINE ('  Partecipanti: ' || v_count_part);

         IF v_count_part > 4
         THEN
            DBMS_OUTPUT.PUT_LINE ('  -> Sufficiente, elaboro da sola.');
            pkg_calcoli_premi_manifest.CALCOLA_PREMIO_FOALS_2025 (
               rec.sequ_id_dati_gara_esterna);
            gare_gia_elaborate (rec.sequ_id_dati_gara_esterna) := 1;
         ELSE
            DECLARE
               v_id_gara_altro   NUMBER := NULL;
            BEGIN
               DBMS_OUTPUT.PUT_LINE (
                  '  -> Troppo pochi, cerco gara da accorpare...');

               FOR alt
                  IN (SELECT dg.sequ_id_dati_gara_esterna
                        FROM tc_dati_gara_esterna dg
                             JOIN
                             TC_DATI_EDIZIONE_ESTERNA ee
                                ON ee.sequ_id_dati_edizione_esterna =
                                      dg.FK_SEQU_ID_DATI_EDIZ_ESTERNA
                       WHERE     dg.sequ_id_dati_gara_esterna !=
                                    rec.sequ_id_dati_gara_esterna
                             AND UPPER (dg.desc_nome_gara_esterna) LIKE
                                    '%FOAL%'
                             AND dg.data_gara_esterna = rec.data_gara_esterna
                             AND ee.DESC_LOCALITA_ESTERA =
                                    rec.DESC_LOCALITA_ESTERA
                             AND dg.fk_sequ_id_gara_manifestazioni
                                    IS NOT NULL)
               LOOP
                  IF NOT gare_gia_elaborate.EXISTS (
                            alt.sequ_id_dati_gara_esterna)
                  THEN
                     v_id_gara_altro := alt.sequ_id_dati_gara_esterna;
                     EXIT;
                  END IF;
               END LOOP;

               IF v_id_gara_altro IS NOT NULL
               THEN
                  DBMS_OUTPUT.PUT_LINE (
                     '  -> Accorpata con gara ID: ' || v_id_gara_altro);
                  gare_gia_elaborate (rec.sequ_id_dati_gara_esterna) := 1;
                  gare_gia_elaborate (v_id_gara_altro) := 1;
                  pkg_calcoli_premi_manifest.CALCOLA_PREMIO_FOALS_2025 (
                     p_id_gara_1   => rec.sequ_id_dati_gara_esterna,
                     p_id_gara_2   => v_id_gara_altro);
               ELSE
                  DBMS_OUTPUT.PUT_LINE (
                     '  -> Nessuna gara compatibile trovata. Procedo da sola.');
                  pkg_calcoli_premi_manifest.CALCOLA_PREMIO_FOALS_2025 (
                     rec.sequ_id_dati_gara_esterna);
                  gare_gia_elaborate (rec.sequ_id_dati_gara_esterna) := 1;
               END IF;
            END;
         END IF;
      END LOOP;

      DBMS_OUTPUT.PUT_LINE ('--- Fine elaborazione gare FOALS ---');
   END ELABORA_PREMI_FOALS_PER_ANNO;



   PROCEDURE CALCOLA_PREMIO_SALTO_OST_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_periodo                      IN     NUMBER,
      p_num_partenti                 IN     NUMBER,
      p_posizione                    IN     NUMBER,
      p_montepremi_tot               IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER)
   AS
      v_montepremi_tot       NUMBER;
      v_percent_premiati     NUMBER;
      v_premiati             NUMBER;
      v_fascia               NUMBER;
      v_tipo_distribuzione   VARCHAR2 (50);
      v_premio_fascia_1      NUMBER := 0;
      v_premio_fascia_2      NUMBER := 0;
      v_premio_fascia_3      NUMBER := 0;
      v_fascia_1             NUMBER;
      v_fascia_2             NUMBER;
      v_fascia_3             NUMBER;
      v_categoria            VARCHAR2 (50);
      v_eta                  NUMBER;

      TYPE tabella_fise IS TABLE OF NUMBER
         INDEX BY PLS_INTEGER;

      v_tabella_fise         tabella_fise;
   BEGIN
      IF FN_GARA_PREMIATA_MASAF (p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA) = 0
      THEN
         p_premio_cavallo := 0;
         RETURN;
      END IF;

      IF UPPER (FN_DESC_TIPOLOGICA (p_dati_gara.FK_CODI_TIPO_CLASSIFICA)) IN
            ('TEMPO', 'BARRAGE')
      THEN
         v_tipo_distribuzione := 'FISE';
      ELSE
         v_tipo_distribuzione := 'MASAF';
      END IF;


      v_categoria := FN_DESC_TIPOLOGICA (p_dati_gara.FK_CODI_CATEGORIA);
      v_eta :=
         TO_NUMBER (
            SUBSTR (FN_DESC_TIPOLOGICA (p_dati_gara.FK_CODI_ETA), 1, 1));

      IF p_montepremi_tot IS NOT NULL
      THEN
         v_montepremi_tot := p_montepremi_tot;
      ELSE
         IF UPPER (v_categoria) = 'SPORT'
         THEN
            v_montepremi_tot := GREATEST (p_num_partenti, 6) * 50;
         ELSIF UPPER (v_categoria) IN ('ELITE', 'ALTO') AND v_eta IN (4, 5)
         THEN
            IF UPPER (v_categoria) = 'ALTO'
            THEN
               v_montepremi_tot := GREATEST (p_num_partenti, 6) * 135;
            ELSE
               v_montepremi_tot := GREATEST (p_num_partenti, 6) * 150;
            END IF;
         ELSIF UPPER (v_categoria) IN ('ELITE', 'ALTO') AND v_eta = 6
         THEN
            v_montepremi_tot :=
               CASE WHEN p_periodo = 1 THEN 2400 ELSE 2900 END;
         ELSIF UPPER (v_categoria) IN ('ELITE', 'ALTO') AND v_eta >= 7
         THEN
            v_montepremi_tot :=
               CASE WHEN v_categoria = 1 THEN 2700 ELSE 3300 END;
         ELSIF UPPER (v_categoria) = 'SELEZIONE'
         THEN
            v_montepremi_tot :=
               CASE
                  WHEN v_eta = 5 THEN 2000
                  WHEN v_eta = 6 THEN 2500
                  ELSE 3000
               END;
         END IF;


         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
                  'DEBUG: Montepremi calcolato dalla procedura  :  '
               || NVL (v_montepremi_tot, -9999));
         END IF;
      END IF;

      IF v_tipo_distribuzione = 'FISE'
      THEN
         v_tabella_fise (1) := 0.25;
         v_tabella_fise (2) := 0.18;
         v_tabella_fise (3) := 0.15;
         v_tabella_fise (4) := 0.12;
         v_tabella_fise (5) := 0.10;

         FOR i IN 6 .. 10
         LOOP
            v_tabella_fise (i) := 0.04;
         END LOOP;

         IF p_posizione <= 10
         THEN
            p_premio_cavallo :=
               ROUND (v_montepremi_tot * v_tabella_fise (p_posizione), 2);
         ELSE
            p_premio_cavallo := 0;
         END IF;

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
               'DEBUG: Premio FISE calcolato: ' || p_premio_cavallo);
         END IF;
      ELSE
         IF UPPER (v_categoria) IN ('ELITE', 'ALTO', 'SELEZIONE')
         THEN
            v_percent_premiati := 0.4;
         ELSE
            v_percent_premiati := 0.5;
         END IF;

         v_premiati := CEIL (p_num_partenti * v_percent_premiati);


         IF v_premiati > 0
         THEN
            v_fascia_1 := FLOOR (v_premiati / 3);
            v_fascia_2 := FLOOR (v_premiati / 3);
            v_fascia_3 := v_premiati - v_fascia_1 - v_fascia_2;
         ELSE
            v_fascia_1 := 0;
            v_fascia_2 := 0;
            v_fascia_3 := 0;
         END IF;


         v_premio_fascia_1 := v_montepremi_tot * 0.5;
         v_premio_fascia_2 := v_montepremi_tot * 0.3;
         v_premio_fascia_3 := v_montepremi_tot * 0.2;

         IF p_posizione <= v_fascia_1 AND v_fascia_1 > 0
         THEN
            v_fascia := 1;
            p_premio_cavallo := ROUND (v_premio_fascia_1 / v_fascia_1, 2);
         ELSIF p_posizione <= (v_fascia_1 + v_fascia_2) AND v_fascia_2 > 0
         THEN
            v_fascia := 2;
            p_premio_cavallo := ROUND (v_premio_fascia_2 / v_fascia_2, 2);
         ELSIF p_posizione <= v_premiati AND v_fascia_3 > 0
         THEN           -- v_premiati è (v_fascia_1 + v_fascia_2 + v_fascia_3)
            v_fascia := 3;
            p_premio_cavallo := ROUND (v_premio_fascia_3 / v_fascia_3, 2);
         ELSE
            v_fascia := 0;
            p_premio_cavallo := 0;
         END IF;

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE ('DEBUG: Fascia premio: ' || v_fascia);
            DBMS_OUTPUT.PUT_LINE (
               'DEBUG: Premio MASAF calcolato: ' || p_premio_cavallo);
         END IF;
      END IF;

      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;

      DBMS_OUTPUT.PUT_LINE (
         'DEBUG: Fine calcolo premio posizione ' || p_posizione);
   EXCEPTION
      WHEN OTHERS
      THEN
         p_premio_cavallo := 0;
         DBMS_OUTPUT.PUT_LINE ('DEBUG ERRORE: ' || SQLERRM);
         RAISE_APPLICATION_ERROR (-20001,
                                  'Errore calcolo premio: ' || SQLERRM);
   END CALCOLA_PREMIO_SALTO_OST_2025;



   PROCEDURE CALCOLA_PREMIO_ENDURANCE_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_categoria                    IN     VARCHAR2,
      p_tipo_evento                  IN     VARCHAR2,
      p_flag_fise                    IN     NUMBER,
      p_posizione                    IN     NUMBER,
      p_montepremi_tot               IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER)
   AS
      v_perc   SYS.odcinumberlist;
      v_base   NUMBER := 0;
   BEGIN
      IF FN_GARA_PREMIATA_MASAF (p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA) = 0
      THEN
         p_premio_cavallo := 0;
         RETURN;
      END IF;


      CASE UPPER (p_tipo_evento)
         WHEN 'FINALE'
         THEN
            v_perc :=
               SYS.odcinumberlist (25,
                                   18,
                                   15,
                                   12,
                                   10,
                                   4,
                                   4,
                                   4,
                                   4,
                                   4);
         WHEN 'CAMPIONATO'
         THEN
            v_perc :=
               SYS.odcinumberlist (29,
                                   22,
                                   19,
                                   16,
                                   14);
         ELSE
            -- Per TAPPA o qualsiasi valore non previsto
            v_perc := SYS.odcinumberlist ();                  -- nessun premio
      END CASE;

      IF p_posizione BETWEEN 1 AND v_perc.COUNT
      THEN
         v_base := p_montepremi_tot * v_perc (p_posizione) / 100;
         p_premio_cavallo :=
            ROUND (v_base / GREATEST (p_num_con_parimerito, 1), 2);
      ELSE
         p_premio_cavallo := 0;
      END IF;

      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;

      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_premio_cavallo := 0;
         DBMS_OUTPUT.PUT_LINE ('DEBUG ERRORE: ' || SQLERRM);
         RAISE_APPLICATION_ERROR (-20001,
                                  'Errore calcolo premio: ' || SQLERRM);
   END CALCOLA_PREMIO_ENDURANCE_2025;



   PROCEDURE CALCOLA_PREMIO_DRESSAGE_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_tipo_evento                  IN     VARCHAR2,
      p_flag_fise                    IN     NUMBER,
      p_livello_cavallo              IN     VARCHAR2,
      p_posizione                    IN     NUMBER,
      p_punteggio                    IN     NUMBER,
      p_montepremi_tot               IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_numero_giornata              IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER)
   IS
      v_perc_premio                    NUMBER;
      v_nome_manifestazione            VARCHAR2 (500);
      l_eta_cavalli                    VARCHAR2 (100);
      v_montepremi_categoria_attuale   NUMBER;
   BEGIN
      v_montepremi_categoria_attuale := p_montepremi_tot;

      IF FN_GARA_PREMIATA_MASAF (p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA) = 0
      THEN
         p_premio_cavallo := 0;
         RETURN;
      END IF;

      -- Estrazione età cavallo e tipo evento
      IF p_dati_gara.fk_codi_eta IS NOT NULL
      THEN
         l_eta_cavalli :=
            TO_NUMBER (
               SUBSTR (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_eta), 0, 1));
      ELSE
         l_eta_cavalli :=
            CASE
               WHEN INSTR (UPPER (p_dati_gara.desc_nome_gara_esterna),
                           '3 ANNI') > 0                                --ANNI
               THEN
                  3
               WHEN INSTR (UPPER (p_dati_gara.desc_nome_gara_esterna),
                           '4 ANNI') > 0                                --ANNI
               THEN
                  4
               WHEN INSTR (UPPER (p_dati_gara.desc_nome_gara_esterna),
                           '5 ANNI') > 0                                --ANNI
               THEN
                  5
               WHEN INSTR (UPPER (p_dati_gara.desc_nome_gara_esterna),
                           '6 ANNI') > 0                                --ANNI
               THEN
                  6
               WHEN INSTR (UPPER (p_dati_gara.desc_nome_gara_esterna),
                           '7 ANNI') > 0                                --ANNI
               THEN
                  7
               ELSE
                  NULL
            END;
      END IF;

      -- Default premio
      p_premio_cavallo := 0;

      -- Recupero riga dalla tabella gara_esterna
      SELECT UPPER (mf.desc_denom_manifestazione)
        INTO v_nome_manifestazione
        FROM TC_DATI_GARA_ESTERNA dg
             JOIN
             TC_DATI_EDIZIONE_ESTERNA ee
                ON ee.sequ_id_dati_edizione_esterna =
                      DG.FK_SEQU_ID_DATI_EDIZ_ESTERNA
             JOIN TC_EDIZIONE ed
                ON ed.sequ_id_edizione = EE.FK_SEQU_ID_EDIZIONE
             JOIN TC_MANIFESTAZIONE mf
                ON mf.sequ_id_manifestazione = ed.fk_sequ_id_manifestazione
       WHERE dg.SEQU_ID_DATI_GARA_ESTERNA =
                p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA;

      --Le prove del primo giorno non ricevono contributi MASAF.

      -- All'interno della procedura CALCOLA_PREMIO_DRESSAGE_2025
      -- ...
      IF v_nome_manifestazione = 'CIRCUITO MASAF DI DRESSAGE'
      THEN                                                      -- È una Tappa
         CASE l_eta_cavalli
            WHEN 4
            THEN
               v_montepremi_categoria_attuale := 2500;
            WHEN 5
            THEN
               v_montepremi_categoria_attuale := 2800;
            WHEN 6
            THEN
               v_montepremi_categoria_attuale := 3200; -- DOVREBBE ESSERE QUESTO
            WHEN 7
            THEN
               v_montepremi_categoria_attuale := 1500;
            WHEN 8
            THEN
               v_montepremi_categoria_attuale := 1500; -- Assumendo 7/8 anni insieme
            ELSE
               v_montepremi_categoria_attuale := 0;
         END CASE;
      ELSIF v_nome_manifestazione = 'FINALE CIRCUITO MASAF DI DRESSAGE'
      THEN                                                     -- È una Finale
         CASE l_eta_cavalli
            WHEN 4
            THEN
               v_montepremi_categoria_attuale := 8000; -- Montepremi Campionato 4 anni
            WHEN 5
            THEN
               v_montepremi_categoria_attuale := 8000; -- Montepremi Campionato 5 anni
            WHEN 6
            THEN
               v_montepremi_categoria_attuale := 8000; -- Montepremi Campionato 6 anni
            WHEN 7
            THEN
               v_montepremi_categoria_attuale := 3000; -- Montepremi Campionato 7/8 anni
            WHEN 8
            THEN
               v_montepremi_categoria_attuale := 3000;
            ELSE
               v_montepremi_categoria_attuale := 0;
         END CASE;
      ELSE
         v_montepremi_categoria_attuale := 0; -- Non è un evento con montepremi Dressage MASAF
      END IF;

      -- ... poi la logica di distribuzione basata su v_montepremi_categoria_attuale e la tabella percentuali

      -- Verifica soglia minima del 60%
      IF p_punteggio < 60
      THEN
         RETURN;
      END IF;

      -- Determina la percentuale da assegnare in base alla posizione (solo primi 5)
      CASE p_posizione
         WHEN 1
         THEN
            v_perc_premio := 35;
         WHEN 2
         THEN
            v_perc_premio := 22;
         WHEN 3
         THEN
            v_perc_premio := 17;
         WHEN 4
         THEN
            v_perc_premio := 14;
         WHEN 5
         THEN
            v_perc_premio := 12;
         ELSE
            v_perc_premio := 0;             -- oltre il 5° posto nessun premio
      END CASE;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('DEBUG: p_tipo_evento: ' || p_tipo_evento);
         DBMS_OUTPUT.PUT_LINE (
               'DEBUG: v_montepremi_categoria_attuale: '
            || v_montepremi_categoria_attuale);
         DBMS_OUTPUT.PUT_LINE ('DEBUG: p_punteggio: ' || p_punteggio);
         DBMS_OUTPUT.PUT_LINE ('DEBUG: p_posizione: ' || p_posizione);
         DBMS_OUTPUT.PUT_LINE ('DEBUG: v_perc_premio: ' || v_perc_premio);
      END IF;

      -- Se c'è una quota da distribuire
      IF v_perc_premio > 0
      THEN
         p_premio_cavallo :=
            ROUND (
                 (v_montepremi_categoria_attuale * v_perc_premio / 100)
               / GREATEST (p_num_con_parimerito, 1),
               2);
      END IF;

      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;
   END;



   PROCEDURE CALCOLA_PREMIO_ALLEV_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_posizione                    IN     NUMBER, -- Posizione originale del cavallo nella classifica della gara
      p_tot_partenti                 IN     NUMBER, -- Numero totale di cavalli che hanno iniziato la prova
      p_num_con_parimerito           IN     NUMBER, -- Numero di cavalli a parimerito in QUESTA posizione
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER)
   IS
      v_montepremi_tot              NUMBER := 0;
      v_tipo_prova_descr            VARCHAR2 (100);
      v_tipo_evento_descr           VARCHAR2 (100);
      v_eta_cavallo_id              NUMBER;
      v_eta_cavallo_num             NUMBER;   -- Età numerica (1, 2, o 3 anni)

      -- Variabili per la logica dei premiati e delle fasce
      v_num_posizioni_da_premiare   NUMBER;
      v_effettivi_premiati_count    NUMBER := 0; -- Conteggio dei cavalli che rientrano nel 50% (inclusi parimerito)

      TYPE t_classificato_rec IS RECORD
      (
         posizione_orig   NUMBER,
         id_cavallo       NUMBER,
         seq_class_est    NUMBER,
         num_parimerito   NUMBER -- Quanti cavalli hanno questa stessa posizione_orig
      );

      TYPE t_classificati_tab IS TABLE OF t_classificato_rec;

      v_classificati_all_gara       t_classificati_tab
                                       := t_classificati_tab ();
      v_effettivi_premiati_tab      t_classificati_tab
                                       := t_classificati_tab (); -- Tabella con solo gli effettivi premiati

      -- Variabili per le fasce (quando >= 7 effettivi premiati)
      v_fascia1_count               NUMBER;
      v_fascia2_count               NUMBER;
      v_fascia3_count               NUMBER;
      v_premio_per_cav_f1           NUMBER;
      v_premio_per_cav_f2           NUMBER;
      v_premio_per_cav_f3           NUMBER;

      v_importo_per_cavallo         NUMBER;
   BEGIN
      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE (
               '--- Inizio CALCOLA_PREMIO_ALLEV_2025 (ID Gara Est: '
            || p_dati_gara.sequ_id_dati_gara_esterna
            || ') per Pos: '
            || p_posizione
            || ' ---');
      END IF;

      p_premio_cavallo := 0;                                        -- Default

      IF FN_GARA_PREMIATA_MASAF (p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA) = 0
      THEN
         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE ('Gara non premiata MASAF.');
         END IF;

         RETURN;
      END IF;

      v_tipo_prova_descr :=
         UPPER (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_tipo_prova));
      v_tipo_evento_descr :=
         UPPER (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_tipo_evento));
      v_eta_cavallo_id := p_dati_gara.fk_codi_eta; -- Assumiamo sia già popolato o dedotto correttamente

      -- Mappatura ID età a numero (semplificata, da adattare se gli ID sono diversi)
      CASE v_eta_cavallo_id
         WHEN 89
         THEN
            v_eta_cavallo_num := 1;                                  -- 1 anno
         WHEN 90
         THEN
            v_eta_cavallo_num := 2;                                  -- 2 anni
         WHEN 91
         THEN
            v_eta_cavallo_num := 3;                                  -- 3 anni
         ELSE
            v_eta_cavallo_num := 0; -- Sconosciuta o non rilevante per alcune prove
      END CASE;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE (
               'Tipo Prova: '
            || v_tipo_prova_descr
            || ', Tipo Evento: '
            || v_tipo_evento_descr
            || ', Età ID: '
            || v_eta_cavallo_id
            || ', Età Num: '
            || v_eta_cavallo_num);
      END IF;

      -- 1. Logica per Classifica Combinata (ha regole di montepremi e distribuzione proprie)
      IF v_tipo_prova_descr = 'CLASSIFICA COMBINATA'
      THEN
         IF v_tipo_evento_descr = 'FINALE'
         THEN
            v_montepremi_tot := 1000;
         ELSE                                      -- Regionale/Interregionale
            IF p_tot_partenti >= 6
            THEN
               v_montepremi_tot := 500;
            ELSE
               v_montepremi_tot := 0; -- Non si assegna premio se < 6 partenti
            END IF;
         END IF;

         IF v_montepremi_tot > 0 AND p_posizione = 1
         THEN
            p_premio_cavallo :=
               ROUND (v_montepremi_tot / GREATEST (p_num_con_parimerito, 1),
                      2);
         ELSE
            p_premio_cavallo := 0;
         END IF;

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
                  'Class. Combinata: Montepremi Tot: '
               || v_montepremi_tot
               || ', Premio Cavallo: '
               || p_premio_cavallo);
         END IF;
      ELSE -- Logica per tutte le altre prove Allevatoriali (Morfo, Salto Lib., Obb., Att. Salto)
         -- 2. Determinazione Montepremi Totale
         IF v_tipo_evento_descr = 'FINALE'
         THEN
            CASE v_tipo_prova_descr
               WHEN 'MORFO-ATTITUDINALE'
               THEN
                  IF v_eta_cavallo_num = 3
                  THEN
                     v_montepremi_tot := 12000;
                  ELSIF v_eta_cavallo_num = 2
                  THEN
                     v_montepremi_tot := 8000;
                  ELSE
                     v_montepremi_tot := 0; -- Finale 1 anno non specificata con montepremi fisso
                  END IF;
               WHEN 'ATTITUDINE AL SALTO'
               THEN
                  IF v_eta_cavallo_num = 2
                  THEN
                     v_montepremi_tot := 6000;
                  ELSE
                     v_montepremi_tot := 0;
                  END IF;
               WHEN 'OBBEDIENZA ED ANDATURE'
               THEN
                  IF v_eta_cavallo_num = 3
                  THEN
                     v_montepremi_tot := 25000;
                  ELSE
                     v_montepremi_tot := 0;
                  END IF;
               WHEN 'SALTO IN LIBERTÀ'
               THEN
                  IF v_eta_cavallo_num = 3
                  THEN
                     v_montepremi_tot := 35000;
                  ELSE
                     v_montepremi_tot := 0;
                  END IF;
               ELSE
                  v_montepremi_tot := 0;
            END CASE;
         ELSE                              -- Tappe o Regionali/Interregionali
            CASE v_tipo_prova_descr
               WHEN 'MORFO-ATTITUDINALE'
               THEN
                  v_importo_per_cavallo :=
                     CASE
                        WHEN v_tipo_evento_descr LIKE '%REGIONALE%' THEN 250
                        ELSE 150
                     END;
               WHEN 'OBBEDIENZA ED ANDATURE'
               THEN
                  v_importo_per_cavallo :=
                     CASE
                        WHEN v_tipo_evento_descr LIKE '%REGIONALE%' THEN 350
                        ELSE 200
                     END;
               WHEN 'SALTO IN LIBERTÀ'
               THEN
                  v_importo_per_cavallo :=
                     CASE
                        WHEN v_tipo_evento_descr LIKE '%REGIONALE%' THEN 350
                        ELSE 200
                     END;
               WHEN 'ATTITUDINE AL SALTO'
               THEN                                          -- Solo Regionali
                  v_importo_per_cavallo := 200;
               ELSE
                  v_importo_per_cavallo := 0;
            END CASE;

            v_montepremi_tot :=
               ROUND (v_importo_per_cavallo * GREATEST (p_tot_partenti, 6),
                      2);
         END IF;

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
               'Montepremi Totale Calcolato: ' || v_montepremi_tot);
         END IF;

         IF v_montepremi_tot = 0
         THEN
            IF c_debug
            THEN
               DBMS_OUTPUT.PUT_LINE ('Montepremi zero, nessun premio.');
            END IF;

            GOTO end_procedure;                  -- Esce se non c'è montepremi
         END IF;

           -- 3. Determinazione degli effettivi premiati (50% dei partecipanti, gestendo parimerito)
           --    Questa parte richiede di avere l'intera classifica della gara.
           --    Assumiamo che venga passata o che si possa recuperare.
           --    Per semplicità qui, lavoreremo su p_tot_partenti, ma nella realtà
           --    bisognerebbe prendere i record da TC_DATI_CLASSIFICA_ESTERNA.

           -- Recupero di TUTTI i classificati della gara per gestire correttamente i parimerito al 50%
           SELECT dce.nume_piazzamento,
                  dce.fk_sequ_id_cavallo,
                  dce.sequ_id_classifica_esterna,
                  COUNT (*) OVER (PARTITION BY dce.nume_piazzamento)
             BULK COLLECT INTO v_classificati_all_gara
             FROM tc_dati_classifica_esterna dce
            WHERE     dce.fk_sequ_id_dati_gara_esterna =
                         p_dati_gara.sequ_id_dati_gara_esterna
                  AND dce.nume_piazzamento IS NOT NULL
         ORDER BY dce.nume_piazzamento ASC;

         v_num_posizioni_da_premiare := CEIL (p_tot_partenti * 0.50);

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
                  'Tot Partenti: '
               || p_tot_partenti
               || ', Num Posizioni da Premiare Teoriche (50%): '
               || v_num_posizioni_da_premiare);
         END IF;

         FOR i IN 1 .. v_classificati_all_gara.COUNT
         LOOP
            IF v_classificati_all_gara (i).posizione_orig <=
                  v_num_posizioni_da_premiare
            THEN
               v_effettivi_premiati_tab.EXTEND;
               v_effettivi_premiati_tab (v_effettivi_premiati_tab.LAST) :=
                  v_classificati_all_gara (i);
            ELSIF     i > 1
                  AND v_classificati_all_gara (i).posizione_orig =
                         v_classificati_all_gara (i - 1).posizione_orig
                  AND v_classificati_all_gara (i - 1).posizione_orig =
                         v_num_posizioni_da_premiare
            THEN
               -- Include i parimerito all'ultima posizione utile
               v_effettivi_premiati_tab.EXTEND;
               v_effettivi_premiati_tab (v_effettivi_premiati_tab.LAST) :=
                  v_classificati_all_gara (i);
            ELSE
               EXIT;  -- Usciamo dal loop, abbiamo raccolto tutti i premiabili
            END IF;
         END LOOP;

         v_effettivi_premiati_count := v_effettivi_premiati_tab.COUNT;

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
                  'Numero Effettivo di Cavalli Premiati (incl. parimerito al 50%): '
               || v_effettivi_premiati_count);
         END IF;

         -- Se il cavallo corrente (identificato da p_SEQU_ID_CLASSIFICA_ESTERNA) non è tra gli effettivi premiati, non prende premio.
         DECLARE
            is_current_cavallo_premiabile   BOOLEAN := FALSE;
         BEGIN
            FOR i IN 1 .. v_effettivi_premiati_tab.COUNT
            LOOP
               IF v_effettivi_premiati_tab (i).seq_class_est =
                     p_SEQU_ID_CLASSIFICA_ESTERNA
               THEN
                  is_current_cavallo_premiabile := TRUE;
                  EXIT;
               END IF;
            END LOOP;

            IF NOT is_current_cavallo_premiabile
            THEN
               IF c_debug
               THEN
                  DBMS_OUTPUT.PUT_LINE (
                        'Cavallo corrente (Seq Est: '
                     || p_SEQU_ID_CLASSIFICA_ESTERNA
                     || ') non è tra gli effettivi premiati.');
               END IF;

               GOTO end_procedure;
            END IF;
         END;


         -- 4. Distribuzione Premi
         IF v_effettivi_premiati_count = 0
         THEN
            IF c_debug
            THEN
               DBMS_OUTPUT.PUT_LINE ('Nessun cavallo premiabile.');
            END IF;

            GOTO end_procedure;
         END IF;

         IF v_effettivi_premiati_count < 7
         THEN
            -- Logica 50-30-20 per i primi 3 effettivi premiati
            DECLARE
               v_pos_attuale_nel_gruppo   NUMBER;
            BEGIN
               -- Trova la posizione del cavallo corrente all'interno del gruppo degli effettivi premiati
               FOR i IN 1 .. v_effettivi_premiati_tab.COUNT
               LOOP
                  IF v_effettivi_premiati_tab (i).seq_class_est =
                        p_SEQU_ID_CLASSIFICA_ESTERNA
                  THEN
                     -- Per gestire i parimerito, consideriamo la posizione originale
                     v_pos_attuale_nel_gruppo :=
                        v_effettivi_premiati_tab (i).posizione_orig;
                     EXIT;
                  END IF;
               END LOOP;

               IF v_pos_attuale_nel_gruppo = 1
               THEN
                  p_premio_cavallo :=
                     ROUND (
                          (v_montepremi_tot * 0.50)
                        / GREATEST (p_num_con_parimerito, 1),
                        2);
               ELSIF v_pos_attuale_nel_gruppo = 2
               THEN
                  p_premio_cavallo :=
                     ROUND (
                          (v_montepremi_tot * 0.30)
                        / GREATEST (p_num_con_parimerito, 1),
                        2);
               ELSIF v_pos_attuale_nel_gruppo = 3
               THEN
                  p_premio_cavallo :=
                     ROUND (
                          (v_montepremi_tot * 0.20)
                        / GREATEST (p_num_con_parimerito, 1),
                        2);
               ELSE
                  p_premio_cavallo := 0;
               END IF;

               IF c_debug
               THEN
                  DBMS_OUTPUT.PUT_LINE (
                        'Effettivi < 7. Pos Orig: '
                     || v_pos_attuale_nel_gruppo
                     || ', Premio: '
                     || p_premio_cavallo);
               END IF;
            END;
         ELSE -- v_effettivi_premiati_count >= 7: divisione in 3 fasce numericamente (quasi) uguali
            -- Calcolo dimensioni delle fasce di cavalli (NON di montepremi)
            v_fascia1_count := FLOOR (v_effettivi_premiati_count / 3);
            v_fascia2_count := FLOOR (v_effettivi_premiati_count / 3);
            v_fascia3_count :=
               v_effettivi_premiati_count - v_fascia1_count - v_fascia2_count;

            -- Per bilanciare meglio, se c'è un resto di 1, va alla fascia 3. Se resto di 2, uno a fascia 3 e uno a fascia 2.
            -- La logica sopra già lo fa implicitamente con FLOOR e sottrazione.

            IF c_debug
            THEN
               DBMS_OUTPUT.PUT_LINE (
                     'Effettivi >= 7. Fascia1 count='
                  || v_fascia1_count
                  || ', Fascia2 count='
                  || v_fascia2_count
                  || ', Fascia3 count='
                  || v_fascia3_count);
            END IF;

            -- Montepremi per fascia
            v_premio_per_cav_f1 :=
               ROUND (
                  (v_montepremi_tot * 0.50) / GREATEST (v_fascia1_count, 1),
                  2);
            v_premio_per_cav_f2 :=
               ROUND (
                  (v_montepremi_tot * 0.30) / GREATEST (v_fascia2_count, 1),
                  2);
            v_premio_per_cav_f3 :=
               ROUND (
                  (v_montepremi_tot * 0.20) / GREATEST (v_fascia3_count, 1),
                  2);

            -- Assicurarsi che premio f1 >= f2 >= f3 (se le fasce sono popolate)
            -- Questa è una semplificazione, la logica 'best_dev' dei FOALS è più robusta per questo.
            -- Per ora, se questo vincolo non è rispettato con la divisione semplice, si potrebbe dare un warning o adottare la 'best_dev'.
            -- In questo contesto, assumiamo che la divisione semplice sia accettabile se il disciplinare non è iper-restrittivo su questo.

            IF     v_fascia2_count > 0
               AND v_premio_per_cav_f1 < v_premio_per_cav_f2
            THEN
               v_premio_per_cav_f2 := v_premio_per_cav_f1;
            END IF;                          -- Non dovrebbe succedere con N/3

            IF     v_fascia3_count > 0
               AND v_premio_per_cav_f2 < v_premio_per_cav_f3
            THEN
               v_premio_per_cav_f3 := v_premio_per_cav_f2;
            END IF;


            -- Determina in quale fascia di cavalli rientra il cavallo corrente (p_posizione)
            -- e assegna il premio unitario di quella fascia.
            -- NB: p_posizione è la posizione originale. Dobbiamo vedere dove si colloca
            -- il cavallo CORRENTE (identificato da p_SEQU_ID_CLASSIFICA_ESTERNA)
            -- all'interno dell'ordinamento degli v_effettivi_premiati_tab.
            DECLARE
               v_idx_in_premiati   NUMBER;
            BEGIN
               FOR i IN 1 .. v_effettivi_premiati_tab.COUNT
               LOOP
                  IF v_effettivi_premiati_tab (i).seq_class_est =
                        p_SEQU_ID_CLASSIFICA_ESTERNA
                  THEN
                     v_idx_in_premiati := i; -- Questo è l'indice del cavallo nella lista dei soli premiati
                     EXIT;
                  END IF;
               END LOOP;

               IF v_idx_in_premiati IS NOT NULL
               THEN
                  IF v_idx_in_premiati <= v_fascia1_count
                  THEN
                     p_premio_cavallo := v_premio_per_cav_f1;
                  ELSIF v_idx_in_premiati <=
                           v_fascia1_count + v_fascia2_count
                  THEN
                     p_premio_cavallo := v_premio_per_cav_f2;
                  ELSE                              -- Deve essere in fascia 3
                     p_premio_cavallo := v_premio_per_cav_f3;
                  END IF;
               ELSE
                  p_premio_cavallo := 0; -- Non dovrebbe succedere se il cavallo è tra i premiabili
               END IF;

               IF c_debug
               THEN
                  DBMS_OUTPUT.PUT_LINE (
                        'Indice cavallo nei premiati: '
                     || v_idx_in_premiati
                     || ', Premio Assegnato: '
                     || p_premio_cavallo);
               END IF;
            END;
         END IF;
      END IF;                          -- Fine logica NON Classifica Combinata

     <<end_procedure>>
      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;

      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('--- Fine CALCOLA_PREMIO_ALLEV_2025 ---');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_premio_cavallo := 0;

         IF c_debug
         THEN
            DBMS_OUTPUT.PUT_LINE (
               'ERRORE in CALCOLA_PREMIO_ALLEV_2025: ' || SQLERRM);
         END IF;
   -- Considera se rilanciare l'eccezione o gestirla
   END CALCOLA_PREMIO_ALLEV_2025;



   PROCEDURE CALCOLA_PREMIO_FOALS_2025 (p_id_gara_1   IN NUMBER,
                                        p_id_gara_2   IN NUMBER DEFAULT NULL)
   AS
      TYPE t_classifica IS RECORD
      (
         rid          ROWID,
         posizione    NUMBER,
         id_cavallo   NUMBER
      );

      TYPE t_classifica_tab IS TABLE OF t_classifica;

      v_risultati      t_classifica_tab;
      v_tot_partenti   NUMBER;
      v_montepremi     NUMBER;
      v_metaparte      NUMBER;
      premiabili       t_classifica_tab;

      fascia1          t_classifica_tab := t_classifica_tab ();
      fascia2          t_classifica_tab := t_classifica_tab ();
      fascia3          t_classifica_tab := t_classifica_tab ();
      premio1          NUMBER := 0;
      premio2          NUMBER := 0;
      premio3          NUMBER := 0;

      v_id_gare        SYS.ODCINUMBERLIST := SYS.ODCINUMBERLIST ();
      v_is_accorpata   BOOLEAN := FALSE;
   BEGIN
      IF c_debug
      THEN
         DBMS_OUTPUT.PUT_LINE ('--- Inizio CALCOLA_PREMI_FOALS 2025---');
         DBMS_OUTPUT.PUT_LINE ('Gara 1: ' || p_id_gara_1);
      END IF;

      IF p_id_gara_2 IS NOT NULL
      THEN
         DBMS_OUTPUT.PUT_LINE ('Gara 2 (accorpata): ' || p_id_gara_2);
         v_id_gare.EXTEND (2);
         v_id_gare (1) := p_id_gara_1;
         v_id_gare (2) := p_id_gara_2;
         v_is_accorpata := TRUE;

         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = 0
          WHERE FK_SEQU_ID_DATI_GARA_ESTERNA IN (p_id_gara_1, p_id_gara_2);
      ELSE
         DBMS_OUTPUT.PUT_LINE ('Nessuna gara accorpata.');
         v_id_gare.EXTEND (1);
         v_id_gare (1) := p_id_gara_1;

         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = 0
          WHERE FK_SEQU_ID_DATI_GARA_ESTERNA IN p_id_gara_1;
      END IF;

      IF v_is_accorpata
      THEN
         DBMS_OUTPUT.PUT_LINE (
            'Accorpamento attivo: classifica ricalcolata per NUME_PUNTI');

           SELECT ROWID,
                  ROW_NUMBER ()
                     OVER (ORDER BY NUME_PUNTI DESC, FK_SEQU_ID_CAVALLO)
                     AS posizione,
                  FK_SEQU_ID_CAVALLO
             BULK COLLECT INTO v_risultati
             FROM TC_DATI_CLASSIFICA_ESTERNA
            WHERE     FK_SEQU_ID_DATI_GARA_ESTERNA IN
                         (SELECT * FROM TABLE (v_id_gare))
                  AND NUME_PUNTI IS NOT NULL
         ORDER BY NUME_PUNTI DESC;

         DBMS_OUTPUT.PUT_LINE ('--- Contenuto di v_risultati ---');

         FOR i IN 1 .. v_risultati.COUNT
         LOOP
            DBMS_OUTPUT.PUT_LINE (
                  'Cavallo ID: '
               || v_risultati (i).id_cavallo
               || ', Posizione: '
               || v_risultati (i).posizione
               || ', ROWID: '
               || v_risultati (i).rid);
         END LOOP;
      ELSE
           SELECT ROWID, NUME_PIAZZAMENTO, FK_SEQU_ID_CAVALLO
             BULK COLLECT INTO v_risultati
             FROM TC_DATI_CLASSIFICA_ESTERNA
            WHERE     FK_SEQU_ID_DATI_GARA_ESTERNA = p_id_gara_1
                  AND NUME_PIAZZAMENTO IS NOT NULL
         ORDER BY NUME_PIAZZAMENTO;
      END IF;

      DBMS_OUTPUT.PUT_LINE (
         'Totale risultati caricati: ' || v_risultati.COUNT);

      v_tot_partenti := v_risultati.COUNT;
      v_montepremi := GREATEST (v_tot_partenti, 6) * 150;
      v_metaparte := FLOOR (v_tot_partenti / 2);

      DBMS_OUTPUT.PUT_LINE (
            'Totale partenti: '
         || v_tot_partenti
         || ', Montepremi: '
         || v_montepremi);

      IF v_tot_partenti < 7
      THEN
         DBMS_OUTPUT.PUT_LINE (
            '- Meno di 7 partenti: applico ripartizione 50/30/20');

         FOR i IN 1 .. v_risultati.COUNT
         LOOP
            IF v_risultati (i).posizione = 1
            THEN
               UPDATE TC_DATI_CLASSIFICA_ESTERNA
                  SET IMPORTO_MASAF_CALCOLATO = ROUND (v_montepremi * 0.50, 2)
                WHERE ROWID = v_risultati (i).rid;
            ELSIF v_risultati (i).posizione = 2
            THEN
               UPDATE TC_DATI_CLASSIFICA_ESTERNA
                  SET IMPORTO_MASAF_CALCOLATO = ROUND (v_montepremi * 0.30, 2)
                WHERE ROWID = v_risultati (i).rid;
            ELSIF v_risultati (i).posizione = 3
            THEN
               UPDATE TC_DATI_CLASSIFICA_ESTERNA
                  SET IMPORTO_MASAF_CALCOLATO = ROUND (v_montepremi * 0.20, 2)
                WHERE ROWID = v_risultati (i).rid;
            END IF;
         END LOOP;

         COMMIT;
         RETURN;
      END IF;


      premiabili := t_classifica_tab ();

      DECLARE
         i             PLS_INTEGER := 1;
         gruppo_temp   t_classifica_tab := t_classifica_tab ();
      BEGIN
         WHILE i <= v_risultati.COUNT
         LOOP
            gruppo_temp.DELETE;

            DECLARE
               curr_pos   NUMBER := v_risultati (i).posizione;
            BEGIN
               WHILE     i <= v_risultati.COUNT
                     AND v_risultati (i).posizione = curr_pos
               LOOP
                  gruppo_temp.EXTEND;
                  gruppo_temp (gruppo_temp.COUNT) := v_risultati (i);
                  i := i + 1;
               END LOOP;
            END;


            IF premiabili.COUNT >= v_metaparte
            THEN
               -- Ho già premiato abbastanza cavalli, esco
               EXIT;
            ELSIF premiabili.COUNT + gruppo_temp.COUNT > v_metaparte
            THEN
               -- Sto per superare la soglia: premio solo se è un gruppo di parimerito
               IF gruppo_temp.COUNT > 1
               THEN
                  FOR j IN 1 .. gruppo_temp.COUNT
                  LOOP
                     DECLARE
                        v_is_masaf   NUMBER;
                     BEGIN
                        SELECT COUNT (*)
                          INTO v_is_masaf
                          FROM tc_cavallo
                         WHERE SEQU_ID_CAVALLO = gruppo_temp (j).id_cavallo;

                        IF v_is_masaf > 0
                        THEN
                           premiabili.EXTEND;
                           premiabili (premiabili.COUNT) := gruppo_temp (j);
                        END IF;
                     END;
                  END LOOP;
               ELSE
                  DBMS_OUTPUT.PUT_LINE (
                     '- NON aggiungo cavallo fuori soglia, non è parimerito.');
               END IF;

               EXIT;
            ELSE
               -- Aggiungo gruppo normalmente
               FOR j IN 1 .. gruppo_temp.COUNT
               LOOP
                  DECLARE
                     v_is_masaf   NUMBER;
                  BEGIN
                     SELECT COUNT (*)
                       INTO v_is_masaf
                       FROM tc_cavallo
                      WHERE SEQU_ID_CAVALLO = gruppo_temp (j).id_cavallo;

                     IF v_is_masaf > 0
                     THEN
                        premiabili.EXTEND;
                        premiabili (premiabili.COUNT) := gruppo_temp (j);
                     END IF;
                  END;
               END LOOP;
            END IF;
         END LOOP;
      END;

      -- Raggruppa parimerito
      DECLARE
         TYPE t_gruppi IS TABLE OF t_classifica_tab
            INDEX BY PLS_INTEGER;

         gruppi       t_gruppi;
         i            PLS_INTEGER := 1;
         gruppo_idx   PLS_INTEGER := 0;
      BEGIN
         DBMS_OUTPUT.PUT_LINE ('premiabili.COUNT ' || premiabili.COUNT);

         WHILE i <= premiabili.COUNT
         LOOP
            DECLARE
               curr_pos      NUMBER := premiabili (i).posizione;
               gruppo_corr   t_classifica_tab := t_classifica_tab ();
            BEGIN
               WHILE     i <= premiabili.COUNT
                     AND premiabili (i).posizione = curr_pos
               LOOP
                  gruppo_corr.EXTEND;
                  gruppo_corr (gruppo_corr.COUNT) := premiabili (i);
                  i := i + 1;
               END LOOP;

               gruppo_idx := gruppo_idx + 1;
               gruppi (gruppo_idx) := gruppo_corr;
            END;
         END LOOP;

         FOR i IN 1 .. gruppi.COUNT
         LOOP
            DBMS_OUTPUT.PUT_LINE (
               'F' || i || ' - Count: ' || gruppi (i).COUNT);
         END LOOP;

         -- Fasce
         IF gruppo_idx = 1
         THEN
            fascia1 := gruppi (1);
            premio1 := ROUND (v_montepremi, 2);
            DBMS_OUTPUT.PUT_LINE ('Premi assegnati:');
            DBMS_OUTPUT.PUT_LINE (
               'Fascia 1: ' || fascia1.COUNT || ' cavalli x ' || premio1);
            RETURN;
         ELSIF gruppo_idx = 2
         THEN
            fascia1 := gruppi (1);
            fascia2 := gruppi (2);
            premio1 :=
               ROUND (v_montepremi * 0.6 / GREATEST (fascia1.COUNT, 1), 2);
            premio2 :=
               ROUND (v_montepremi * 0.4 / GREATEST (fascia2.COUNT, 1), 2);
            DBMS_OUTPUT.PUT_LINE ('Premi assegnati:');
            DBMS_OUTPUT.PUT_LINE (
               'Fascia 1: ' || fascia1.COUNT || ' cavalli x ' || premio1);
            DBMS_OUTPUT.PUT_LINE (
               'Fascia 2: ' || fascia2.COUNT || ' cavalli x ' || premio2);
            RETURN;
         END IF;

         -- Combinazione ottimizzata per 3 fasce
         DECLARE
            best_dev        NUMBER := NULL;
            found_ok        BOOLEAN := FALSE;
            total_cavalli   PLS_INTEGER := premiabili.COUNT;
            best_f1         t_classifica_tab;
            best_f2         t_classifica_tab;
            best_f3         t_classifica_tab;
            best_p1         NUMBER;
            best_p2         NUMBER;
            best_p3         NUMBER;
         BEGIN
            FOR i IN 1 .. gruppo_idx - 2
            LOOP
               FOR j IN i + 1 .. gruppo_idx - 1
               LOOP
                  DECLARE
                     f1     t_classifica_tab := t_classifica_tab ();
                     f2     t_classifica_tab := t_classifica_tab ();
                     f3     t_classifica_tab := t_classifica_tab ();
                     cnt1   PLS_INTEGER := 0;
                     cnt2   PLS_INTEGER := 0;
                     cnt3   PLS_INTEGER := 0;
                  BEGIN
                     FOR g IN 1 .. gruppo_idx
                     LOOP
                        FOR c IN 1 .. gruppi (g).COUNT
                        LOOP
                           IF g <= i
                           THEN
                              f1.EXTEND;
                              f1 (f1.COUNT) := gruppi (g) (c);
                              cnt1 := cnt1 + 1;
                           ELSIF g <= j
                           THEN
                              f2.EXTEND;
                              f2 (f2.COUNT) := gruppi (g) (c);
                              cnt2 := cnt2 + 1;
                           ELSE
                              f3.EXTEND;
                              f3 (f3.COUNT) := gruppi (g) (c);
                              cnt3 := cnt3 + 1;
                           END IF;
                        END LOOP;
                     END LOOP;

                     DECLARE
                        p1       NUMBER
                           := ROUND (
                                 (v_montepremi * 0.5) / GREATEST (cnt1, 1),
                                 2);
                        p2       NUMBER
                           := ROUND (
                                 (v_montepremi * 0.3) / GREATEST (cnt2, 1),
                                 2);
                        p3       NUMBER
                           := ROUND (
                                 (v_montepremi * 0.2) / GREATEST (cnt3, 1),
                                 2);
                        target   NUMBER := total_cavalli / 3;
                        dev      NUMBER
                           :=   POWER (cnt1 - target, 2)
                              + POWER (cnt2 - target, 2)
                              + POWER (cnt3 - target, 2);
                     BEGIN
                        IF p1 >= p2 AND p2 >= p3
                        THEN
                           IF best_dev IS NULL OR dev < best_dev
                           THEN
                              best_dev := dev;
                              best_f1 := f1;
                              best_f2 := f2;
                              best_f3 := f3;
                              best_p1 := p1;
                              best_p2 := p2;
                              best_p3 := p3;
                              found_ok := TRUE;
                           END IF;
                        END IF;
                     END;
                  END;
               END LOOP;
            END LOOP;

            IF NOT found_ok
            THEN
               RAISE_APPLICATION_ERROR (
                  -20001,
                  'Nessuna combinazione valida per 3 fasce.');
            END IF;

            fascia1 := best_f1;
            fascia2 := best_f2;
            fascia3 := best_f3;
            premio1 := best_p1;
            premio2 := best_p2;
            premio3 := best_p3;

            DBMS_OUTPUT.PUT_LINE ('--- Fascia 1 ---');

            FOR i IN 1 .. fascia1.COUNT
            LOOP
               DBMS_OUTPUT.PUT_LINE (
                     'F1 - Cavallo ID: '
                  || fascia1 (i).id_cavallo
                  || ', ROWID: '
                  || fascia1 (i).rid);
            END LOOP;

            DBMS_OUTPUT.PUT_LINE ('--- Fascia 2 ---');

            FOR i IN 1 .. fascia2.COUNT
            LOOP
               DBMS_OUTPUT.PUT_LINE (
                     'F2 - Cavallo ID: '
                  || fascia2 (i).id_cavallo
                  || ', ROWID: '
                  || fascia2 (i).rid);
            END LOOP;

            DBMS_OUTPUT.PUT_LINE ('--- Fascia 3 ---');

            FOR i IN 1 .. fascia3.COUNT
            LOOP
               DBMS_OUTPUT.PUT_LINE (
                     'F3 - Cavallo ID: '
                  || fascia3 (i).id_cavallo
                  || ', ROWID: '
                  || fascia3 (i).rid);
            END LOOP;
         END;
      END;


      FOR i IN 1 .. fascia1.COUNT
      LOOP
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = premio1
          WHERE ROWID = fascia1 (i).rid;

         DBMS_OUTPUT.PUT_LINE ('Aggiorno fascia 1 premio:' || premio1);
      END LOOP;

      FOR i IN 1 .. fascia2.COUNT
      LOOP
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = premio2
          WHERE ROWID = fascia2 (i).rid;

         DBMS_OUTPUT.PUT_LINE ('Aggiorno fascia 2 premio:' || premio2);
      END LOOP;

      FOR i IN 1 .. fascia3.COUNT
      LOOP
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = premio3
          WHERE ROWID = fascia3 (i).rid;

         DBMS_OUTPUT.PUT_LINE ('Aggiorno fascia 3 premio:' || premio3);
      END LOOP;

      COMMIT;
      DBMS_OUTPUT.PUT_LINE ('--- Fine CALCOLA_PREMI_FOALS ---');
   END CALCOLA_PREMIO_FOALS_2025;

   PROCEDURE CALCOLA_PREMIO_COMPLETO_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_posizione                    IN     NUMBER,
      p_tot_partenti                 IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER)
   IS
      v_eta_cavallo        NUMBER;
      v_tipo_evento        VARCHAR2 (100);
      v_montepremi_tot     NUMBER := 0;
      v_percent_premiati   NUMBER;
      v_num_premiati       NUMBER;
      v_fascia_1           NUMBER;
      v_fascia_2           NUMBER;
      v_fascia_3           NUMBER;
      v_premio_fascia_1    NUMBER;
      v_premio_fascia_2    NUMBER;
      v_premio_fascia_3    NUMBER;
   BEGIN
      IF FN_GARA_PREMIATA_MASAF (p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA) = 0
      THEN
         p_premio_cavallo := 0;
         RETURN;
      END IF;

      -- Estrazione età cavallo e tipo evento
      v_eta_cavallo :=
         TO_NUMBER (
            SUBSTR (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_eta), 1, 1));
      v_tipo_evento :=
         UPPER (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_tipo_evento));

      -- Calcolo montepremi secondo il tipo evento
      CASE v_tipo_evento
         WHEN 'TAPPA'
         THEN
            CASE v_eta_cavallo
               WHEN 4
               THEN
                  v_montepremi_tot := GREATEST (p_tot_partenti, 6) * 50;
               WHEN 5
               THEN
                  v_montepremi_tot := GREATEST (p_tot_partenti, 6) * 75;
            END CASE;
         WHEN 'FINALE'
         THEN
            CASE v_eta_cavallo
               WHEN 4
               THEN
                  v_montepremi_tot := 1500;
               WHEN 5
               THEN
                  v_montepremi_tot := 3000;
            END CASE;
         WHEN 'CAMPIONATO 6 ANNI'
         THEN
            v_montepremi_tot := 5000;
         WHEN 'CAMPIONATO 7 ANNI'
         THEN
            v_montepremi_tot := 8000;
         ELSE
            v_montepremi_tot := 0;
      END CASE;

      -- Premiabili
      v_percent_premiati := CASE WHEN v_eta_cavallo >= 6 THEN 0.4 ELSE 0.5 END;

      v_num_premiati := CEIL (p_tot_partenti * v_percent_premiati);
      v_fascia_1 := FLOOR (v_num_premiati / 3);
      v_fascia_2 := FLOOR (v_num_premiati / 3);
      v_fascia_3 := v_num_premiati - v_fascia_1 - v_fascia_2;

      v_premio_fascia_1 := v_montepremi_tot * 0.5;
      v_premio_fascia_2 := v_montepremi_tot * 0.3;
      v_premio_fascia_3 := v_montepremi_tot * 0.2;

      -- Ridistribuzione fasce vuote
      IF v_fascia_2 = 0
      THEN
         v_premio_fascia_1 := v_premio_fascia_1 + v_premio_fascia_2;
         v_premio_fascia_2 := 0;
      END IF;

      IF v_fascia_3 = 0
      THEN
         IF v_fascia_2 > 0
         THEN
            v_premio_fascia_2 := v_premio_fascia_2 + v_premio_fascia_3;
         ELSE
            v_premio_fascia_1 := v_premio_fascia_1 + v_premio_fascia_3;
         END IF;
      END IF;

      -- Assegnazione premio in base alla posizione
      IF p_posizione <= v_fascia_1
      THEN
         p_premio_cavallo :=
            ROUND (v_premio_fascia_1 / p_num_con_parimerito, 2);
      ELSIF p_posizione <= (v_fascia_1 + v_fascia_2)
      THEN
         p_premio_cavallo :=
            ROUND (v_premio_fascia_2 / p_num_con_parimerito, 2);
      ELSIF p_posizione <= v_num_premiati
      THEN
         p_premio_cavallo :=
            ROUND (v_premio_fascia_3 / p_num_con_parimerito, 2);
      ELSE
         p_premio_cavallo := 0;
      END IF;

      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_premio_cavallo := 0;
         DBMS_OUTPUT.PUT_LINE ('Errore: ' || SQLERRM);
   END CALCOLA_PREMIO_COMPLETO_2025;


   PROCEDURE CALCOLA_PREMIO_MONTA_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_posizione                    IN     NUMBER,
      p_tot_partenti                 IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER)
   AS
      v_categoria_nome        VARCHAR2 (100);
      v_tipo_evento           VARCHAR2 (100);
      v_montepremi_tot        NUMBER := 0;
      v_montepremi_cat        NUMBER := 0;
      v_percentuale           NUMBER := 0;
      v_nome_manifestazione   VARCHAR2 (255);
      l_desc_formula          VARCHAR2 (50);
   BEGIN
      IF FN_GARA_PREMIATA_MASAF (p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA) = 0
      THEN
         p_premio_cavallo := 0;
         RETURN;
      END IF;

      -- Recupero riga dalla tabella gara_esterna
      SELECT UPPER (mf.desc_denom_manifestazione), UPPER (mf.desc_formula)
        INTO v_nome_manifestazione, l_desc_formula
        FROM TC_DATI_GARA_ESTERNA dg
             JOIN
             TC_DATI_EDIZIONE_ESTERNA ee
                ON ee.sequ_id_dati_edizione_esterna =
                      DG.FK_SEQU_ID_DATI_EDIZ_ESTERNA
             JOIN TC_EDIZIONE ed
                ON ed.sequ_id_edizione = EE.FK_SEQU_ID_EDIZIONE
             JOIN TC_MANIFESTAZIONE mf
                ON mf.sequ_id_manifestazione = ed.fk_sequ_id_manifestazione
       WHERE dg.SEQU_ID_DATI_GARA_ESTERNA =
                p_dati_gara.SEQU_ID_DATI_GARA_ESTERNA;

      -- Estrazione della categoria e tipo evento
      -- v_categoria_nome :=
      --    UPPER (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_categoria));
      -- v_tipo_evento :=
      --    UPPER (FN_DESC_TIPOLOGICA (p_dati_gara.fk_codi_tipo_evento));

      -- Determina il montepremi totale in base all'evento
      IF v_nome_manifestazione LIKE '%FINALE%'
      THEN
         v_montepremi_tot := 12000;
      ELSE
         v_montepremi_tot := 4000;
      END IF;

      DBMS_OUTPUT.PUT_LINE ('v_montepremi_tot: ' || v_montepremi_tot);
      -- Ogni categoria riceve il 20% del montepremi totale
      v_montepremi_cat := v_montepremi_tot * 0.2;

      -- Percentuale premio in base alla posizione
      CASE p_posizione
         WHEN 1
         THEN
            v_percentuale := 0.30;
         WHEN 2
         THEN
            v_percentuale := 0.25;
         WHEN 3
         THEN
            v_percentuale := 0.20;
         WHEN 4
         THEN
            v_percentuale := 0.15;
         WHEN 5
         THEN
            v_percentuale := 0.10;
         ELSE
            p_premio_cavallo := 0;
            RETURN;
      END CASE;

      -- Calcolo del premio
      p_premio_cavallo :=
         ROUND ( (v_montepremi_cat * v_percentuale) / p_num_con_parimerito,
                2);

      IF p_SEQU_ID_CLASSIFICA_ESTERNA IS NOT NULL
      THEN
         UPDATE TC_DATI_CLASSIFICA_ESTERNA
            SET IMPORTO_MASAF_CALCOLATO = p_premio_cavallo
          WHERE SEQU_ID_CLASSIFICA_ESTERNA = p_SEQU_ID_CLASSIFICA_ESTERNA;

         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_premio_cavallo := 0;
         DBMS_OUTPUT.PUT_LINE ('Errore: ' || SQLERRM);
   END CALCOLA_PREMIO_MONTA_2025;
END PKG_CALCOLI_PREMI_MANIFEST;
/