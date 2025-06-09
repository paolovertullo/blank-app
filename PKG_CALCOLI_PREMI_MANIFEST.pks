CREATE OR REPLACE PACKAGE UNIRE_REL2.PKG_CALCOLI_PREMI_MANIFEST
AS
   TYPE t_premio_rec IS RECORD
   (
      cavallo_id     NUMBER,
      nome_cavallo   VARCHAR2 (100),
      premio         NUMBER,
      posizione      NUMBER,
      note           VARCHAR2 (500)
   );
 
   TYPE t_tabella_premi IS TABLE OF t_premio_rec;



   -- Restituisce una tabella premi per una gara specifica,
   -- determinando automaticamente la disciplina e richiamando l'handler corretto.
   FUNCTION FN_CALCOLO_PREMI_MANIFEST (p_gara_id IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   -- Simula il calcolo premi di una gara, senza salvarne l¿esito.
   -- Utilizzato a scopo previsionale.
   FUNCTION FN_CALCOLO_PREMI_MANIFEST_SIM (p_gara_id        IN NUMBER,
                                           p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   -- Restituisce la descrizione di un codice tipologico.
   FUNCTION FN_DESC_TIPOLOGICA (p_codice IN NUMBER)
      RETURN VARCHAR2;

   -- Verifica se una gara è premiata dal MASAF.0 non è premiata 1 è premiata
   FUNCTION FN_GARA_PREMIATA_MASAF (p_id_gara_esterna IN NUMBER)
      RETURN NUMBER;

   FUNCTION FN_INCENTIVO_MASAF_GARA_FISE (p_id_gara_esterna IN NUMBER)
      RETURN NUMBER;

   -- Ricava la disciplina associata ad una gara esterna.
   FUNCTION GET_DISCIPLINA (p_gara_id IN NUMBER)
      RETURN NUMBER;

   -- Restituisce tutte le informazioni associate ad una gara esterna.
   FUNCTION FN_INFO_GARA_ESTERNA (p_gara_id IN NUMBER)
      RETURN tc_dati_gara_esterna%ROWTYPE;

   TYPE tc_dati_gara_esterna_obj IS RECORD
   (
      fk_sequ_id_dati_ediz_esterna     NUMBER (12),
      sequ_id_dati_gara_esterna        NUMBER (12),
      fk_sequ_id_gara_manifestazioni   NUMBER (12),
      codi_gara_esterna                VARCHAR2 (10),
      desc_nome_gara_esterna           VARCHAR2 (100),
      data_gara_esterna                CHAR (8),
      desc_gruppo_categoria            VARCHAR2 (50),
      desc_codice_categoria            VARCHAR2 (50),
      desc_altezza_ostacoli            VARCHAR2 (50),
      flag_gran_premio                 NUMBER (1),
      codi_utente_inserimento          VARCHAR2 (20),
      dttm_inserimento                 DATE,
      codi_utente_aggiornamento        VARCHAR2 (20),
      dttm_aggiornamento               DATE,
      flag_prova_a_squadre             NUMBER (1),
      nume_mance                       NUMBER (2),
      codi_prontuario                  VARCHAR2 (50),
      nume_cavalli_italiani            VARCHAR2 (5),
      desc_formula                     VARCHAR2 (50),
      data_dressage                    VARCHAR2 (8),
      data_cross                       VARCHAR2 (8),
      fk_codi_categoria                NUMBER,
      fk_codi_tipo_classifica          NUMBER,
      fk_codi_livello_cavallo          NUMBER,
      fk_codi_tipo_evento              NUMBER,
      fk_codi_tipo_prova               NUMBER,
      fk_codi_regola_sesso             NUMBER,
      fk_codi_regola_libro             NUMBER,
      fk_codi_eta                      NUMBER,
      flag_premio_masaf                NUMBER (1)
   );

   TYPE tc_dati_gara_esterna_tbl
      IS TABLE OF UNIRE_REL2.TC_DATI_GARA_ESTERNA%ROWTYPE;

   FUNCTION FN_INFO_GARA_SQL (p_gara_id IN NUMBER)
      RETURN tc_dati_gara_esterna_tbl
      PIPELINED;

   FUNCTION FN_PERIODO_SALTO_OSTACOLI (p_data_gara VARCHAR2)
      RETURN NUMBER;


   -- Simulazioni premi per ciascuna disciplina MASAF.
   FUNCTION FN_CALCOLO_SALTO_OSTACOLI_SIM (p_gara_id        IN NUMBER,
                                           p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   FUNCTION FN_CALCOLO_DRESSAGE_SIM (p_gara_id        IN NUMBER,
                                     p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   FUNCTION FN_CALCOLO_ENDURANCE_SIM (p_gara_id        IN NUMBER,
                                      p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   FUNCTION FN_CALCOLO_ALLEVATORIALE_SIM (p_gara_id        IN NUMBER,
                                          p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   FUNCTION FN_CALCOLO_COMPLETO_SIM (p_gara_id        IN NUMBER,
                                     p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   FUNCTION FN_CALCOLO_MONTA_DA_LAVORO_SIM (p_gara_id        IN NUMBER,
                                            p_num_partenti   IN NUMBER)
      RETURN t_tabella_premi
      PIPELINED;

   -- Handler di calcolo premi per disciplina
   FUNCTION HANDLER_SALTO_OSTACOLI (p_gara_id IN NUMBER)
      RETURN t_tabella_premi;

   FUNCTION HANDLER_DRESSAGE (p_gara_id IN NUMBER)
      RETURN t_tabella_premi;

   FUNCTION HANDLER_ENDURANCE (p_gara_id IN NUMBER)
      RETURN t_tabella_premi;

   FUNCTION HANDLER_ALLEVATORIALE (p_gara_id IN NUMBER)
      RETURN t_tabella_premi;

   FUNCTION HANDLER_COMPLETO (p_gara_id IN NUMBER)
      RETURN t_tabella_premi;

   FUNCTION HANDLER_MONTA_DA_LAVORO (p_gara_id IN NUMBER)
      RETURN t_tabella_premi;

   -- Dispatcher principale: invoca il calcolo premi per la gara specificata
   -- e restituisce i premi in uscita in formato tabellare.
   --non dovrebbe servire piu' l'output dei risultati
   PROCEDURE ELABORA_PREMI_GARA (p_gara_id         IN     NUMBER,
                                 p_forza_elabora   IN     NUMBER,
                                 p_risultato          OUT VARCHAR2);

   --p_risultati OUT t_tabella_premi);

   -- Procedura per l'elaborazione dei premi FOALS per un intero anno.
   -- Gestisce la logica per l'accorpamento delle classifiche se necessario.
   PROCEDURE ELABORA_PREMI_FOALS_PER_ANNO (v_anno IN VARCHAR2);

   -- Procedure di calcolo premio per cavallo (per disciplina e anno)
   PROCEDURE CALCOLA_PREMIO_SALTO_OST_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_periodo                      IN     NUMBER,
      p_num_partenti                 IN     NUMBER,
      p_posizione                    IN     NUMBER,
      p_montepremi_tot               IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER);

   PROCEDURE CALCOLA_PREMIO_ENDURANCE_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_categoria                    IN     VARCHAR2,
      p_tipo_evento                  IN     VARCHAR2,
      p_flag_fise                    IN     NUMBER,
      p_posizione                    IN     NUMBER,
      p_montepremi_tot               IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER);

   PROCEDURE CALCOLA_PREMIO_DRESSAGE_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_tipo_evento                  IN     VARCHAR2,
      p_flag_fise                    IN     NUMBER, -- 1 se FISE, 0 altrimenti
      p_livello_cavallo              IN     VARCHAR2,
      p_posizione                    IN     NUMBER,
      p_punteggio                    IN     NUMBER,
      p_montepremi_tot               IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_numero_giornata              IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER);

   PROCEDURE CALCOLA_PREMIO_ALLEV_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_posizione                    IN     NUMBER,
      p_tot_partenti                 IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER);

   -- Calcola e registra i premi FOALS per una o due gare specifiche.
   -- Se il numero di partenti è insufficiente, gestisce la classifica unica.
   PROCEDURE CALCOLA_PREMIO_FOALS_2025 (p_id_gara_1   IN NUMBER,
                                        p_id_gara_2   IN NUMBER DEFAULT NULL);

   PROCEDURE CALCOLA_PREMIO_COMPLETO_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_posizione                    IN     NUMBER,
      p_tot_partenti                 IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER);

   PROCEDURE CALCOLA_PREMIO_MONTA_2025 (
      p_dati_gara                    IN     tc_dati_gara_esterna%ROWTYPE,
      p_posizione                    IN     NUMBER,
      p_tot_partenti                 IN     NUMBER,
      p_num_con_parimerito           IN     NUMBER,
      p_SEQU_ID_CLASSIFICA_ESTERNA   IN     NUMBER,
      p_premio_cavallo                  OUT NUMBER);
END PKG_CALCOLI_PREMI_MANIFEST;
/