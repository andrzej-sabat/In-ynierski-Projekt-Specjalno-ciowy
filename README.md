# Grupa 4
# Benchmark wydajności bazy Clickhouse oraz Neo4J
## Aplikacja spełniajaca zadania dziennika elektronicznego przygotowana na zajęcia z programowania zespołowego.

## Spis treści:
1. Cel systemu,
2. Potencjalni użytkownicy systemu,
3. Informacje gromadzone przez system,
4. Informacje generowane przez system,
5. Uprawnienia użytkowników,
6. Autorzy aplikacji,
7. Licencja,
8. Schematy UML systemu.



#### 1. Cel systemu: 
*Aplikacja pozwala na komunikacje z bazą danych Clickhouse oraz Neo4J. Użytkownik może przełączać sie pomiedzy silnikami bazodanowymi i zasilać je zewnetrznymi danymi. Po zasileniu jednego z silników użytkownik może przetransferować dane do drugiego silnika. System pozawala na wybór tabeli której zawartość wraz ze struktórą przenoszona jest między tabelami a także wykonuje proste zapytania na danych w zależności od wybranego silnika. Wszystkie operacje wykonywane w systemie generują współczynniki czasowe pozwalające na weryfikację wydajności działania danego silnika bazodanowego.*

#### 2. Potencjalni użytkownicy systemu:
*System kierowany jest do osób chcących porównać wydajność silników bazodanowych. W systemie istnieje tylko jedna grupa użytkownikó która ma dostęp do wszystkich części składowych aplikacji.*

#### 3. Informacje gromadzone przez system:
*System gromadzi informacje dotyczące czasu wykonania operacji benchmarkowych dla danego silnika*

#### 4. Informacje generowane przez system:
*Aplikacja generuje zapytania do danych w systemie. Czas wykonania zapytań jest podstawową informacją, która pozwala na weryfikacje wydajności danego silnika bazy danych.*

#### 5. Uprawnienia użytkowników:
*W systemie istnieje jedna grupa użytkowników, która ma dostęp do wszystkich funkcjonalności systemu.*

#### 6.Autorzy aplikacji:
- Andrzej Sabat
- Adam Marzec
- Damian Wawrzkowicz
- Krzysztof Pięta

#### 7.Licencja

Aplikacją jest objęta licencją MIT.




#### 8. Schematy UML systemu:
- diagram przypadków użycia

![Błąd](diagram_przypadków_uzyciaa.png "Opcjonalny tytul")


- diagram klas
 
 
 ![Błąd](diagram_klas.png "Opcjonalny tytul")
 
 
 - diagram aktywności
 
 ![Błąd](diagram_aktywnosci.png "Opcjonalny tytul")
 
 - diagram sekwencji


![Błąd](Diagram_sekwencji.png "Opcjonalny tytul")

 - diagram ERD bazy danych
 
 ![Błąd](ERD_baza_danych.PNG "Opcjonalny tytul")
