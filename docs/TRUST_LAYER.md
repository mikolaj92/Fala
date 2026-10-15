# Warstwa zaufania: werdykt eksploracji

## Tożsamość i odbiór

Repozytorium: `mikolaj92/Fala`, gałąź `lokay/288`.
Odczyt kodu dotyczy SHA `32380e8fa7c1e45b71c53fe08acf1e570c908dfd`,
drzewa `ffead29b612cb6430f8a5faf89e593995bdefff3`.
Początkowe drzewo robocze było czyste, również `pixi.lock`.
Nie istniał ten dokument ani `mojo/fala/exerciser.mojo`.
Poniższe dowody ze sprawdzianów oznaczają odczyt, nie zaliczone wykonanie.

**Decyzje: 5/5. Odbiór wykonawczy: ZABLOKOWANY.**
Nie zmieniamy runtime ani zależności bez wykazanej usterki CI.
Utworzenie issue potomnych pozostaje niezweryfikowane.

## Pięć decyzji

| Kierunek | Werdykt | Stan istniejący i uzasadnienie |
| --- | --- | --- |
| Naruszenia kontraktu | Przyjęte; zachować implementację | `violations.mojo`, `native_driver.mojo` i `journal.mojo` zapisują przypisaną winę. Istnieją sprawdziany `native_subprocess.mojo` i `native_driver.mojo`. |
| Exerciser | Przyjąć jako dalszy kierunek, nie jako gotowy organ | Brak modułu, zadania pixi, generatora impulsów i reduktora Mojo. `python/fala/conformance.py:exercise_handler` sprawdza podany przykład; nie generuje sesji. Przypadki poprawnej i błędnej odpowiedzi istnieją w `python/tests/test_conformance_checkers.py`. |
| TLA+/TLC | Odroczyć; zero nowych zależności | Brak generatora specyfikacji, modelu i zadania TLC w sprawdzonym drzewie. Brak dowodu sprawdzenia modelu. |
| Porównywanie organów | Odroczyć | `tee_journal.mojo` powiela zapisy dwóch dzienników pamięciowych, nie uruchamia dwóch organów ani nie porównuje reakcji. `mojo/smoke/tee_journal.mojo` sprawdza dziennik, nie niezależną wyrocznię. |
| Reguły ładowania pakietu | Odroczyć nową bramkę grafową i Datalog | Istnieje walidacja manifestu w `native_package.mojo`, ale nie dowodzi wszystkich reguł architektury z prozy. Dowody: `native_manifest.mojo` i `output_contracts.mojo` w `mojo/smoke/`. |

## Kontrakty, właściciele i kolejność

Wszystkie poniższe pliki runtime znajdują się w `mojo/fala/`.

| Granica | Właściciel walidacji | Rzeczywista para command → event | Istniejący sprawdzian |
| --- | --- | --- | --- |
| Koperta subprocessu, brak wyniku | `adapters.mojo:collect_subprocess`, `effector_protocol.mojo`; zapis przez driver | `violation.record` → `violation.recorded` | `mojo/smoke/native_subprocess.mojo` |
| Koperta tworzona przez rodzica dla native_function | `adapters.mojo:execute_native_function`; przypisanie winy przez driver | `violation.record` → `violation.recorded` | `mojo/smoke/native_driver.mojo`, przypadek `driver-correlator` |
| Wynik domenowy przed przekazaniem dalej | `journal.mojo:complete_process`, walidacja projekcji w `correlation_advance.mojo` | `process.complete` → `process.completed`; odrzucenie: `violation.record` → `violation.recorded`, następnie `process.fail` → `process.failed` | `mojo/smoke/output_contracts.mojo`, `python/tests/test_output_contracts.py`, `mojo/smoke/native_subprocess.mojo` |
| Ponowienie nieudanego wykonania | `native_driver.mojo` i `journal.mojo` | `process.retry` → `process.retry_scheduled` | `mojo/smoke/native_driver.mojo`, `mojo/smoke/native_subprocess.mojo` |

`violations.mojo:is_contract_violation` obejmuje `adapter_invalid_result`,
`adapter_missing_output`, `usage_invalid` i `output_schema_invalid`.
Błędny JSON przechodzi przez walidację koperty w `collect_subprocess`.
Odczytany przypadek `liar` sprawdza poprawny JSON z błędną kopertą,
nie osobny trwały zapis dla składniowo błędnego JSON. Ten dowód pozostaje luką.

Driver przypisuje `correlator` błędowi `adapter_invalid_result` dla `native_function`.
Pozostałe rejestrowane naruszenia otrzymują `effector`.
To konkretna reguła przypisania winy, nie ogólny dowód źródła każdego błędu.
Wyjście niezerowe i timeout to awarie wykonania, nie naruszenia kontraktu.
`native_subprocess.mojo` sprawdza dla nich brak `violation.recorded`.
`durable_subprocess.mojo` obsługuje anulowanie procesu; nie jest walidatorem odpowiedzi.
Sprawdzian o tej samej nazwie sprawdza anulowanie i usunięcie grupy procesów.

Driver odrzuca błędną odpowiedź, zapisuje naruszenie, a potem kończy lub ponawia próbę.
Walidacja schematu w `complete_process` poprzedza zapis zakończenia sukcesem.
Istniejący sprawdzian Pythona potwierdza oczekiwanie: błędne źródło pomija konsumenta.
Zapis naruszenia i późniejsza zmiana statusu są osobnymi transakcjami.
Nie deklarujemy ich wspólnej atomowości ani odporności na awarię pomiędzy nimi.

## Trwałość, replay i niezależność dowodu

`journal.mojo:record_violation` używa klucza
`violation.record:<process_id>:<attempt>` w zakresie `run_id`.
`submit_command` zapisuje polecenie i jego zdarzenia w jednej transakcji SQLite.
Powtórzony klucz zwraca zapisane polecenie bez nowych zdarzeń;
ta ścieżka nie porównuje ponownie treści payloadu.
`native_subprocess.mojo` sprawdza jeden wpis naruszenia i replay tej samej próby.
Nie sprawdza tego replay po ponownym otwarciu ani liczników wywołań efektora i wszystkich zapisów.

`run_identity_replay.mojo` sprawdza zgodność tożsamości, odrzucenie rozbieżności
oraz zero taktów drivera przy terminalnym replay na otwartym dzienniku.
Nie mierzy jednocześnie zerowego przyrostu wywołań i zapisów po ponownym otwarciu.
`sqlite_journal_port.mojo` w `mojo/smoke/` sprawdza replay create/schedule na tym samym połączeniu.
Nie sprawdza awarii dysku ani ponownego otwarcia.
To nazwane luki, nie potwierdzenie dokładnie jednokrotnego wykonania.

`journal_port.mojo` definiuje struktury granicy trwałości, nie jej dowód dla wszystkich adapterów.
`SqliteJournalPort.append_batch` obsługuje wiodącą jednostkę; nie zapisuje dowolnej listy jednostek jako jednej transakcji.
Własności transakcji `NativeJournal` nie przechodzą automatycznie na TeeJournal lub inne adaptery.

Oczekiwania w `output_contracts.mojo` zawierają jawne poprawne i błędne wartości,
ale oba sprawdzane tory korzystają ze wspólnej walidacji schematu.
Nie jest to niezależna wyrocznia ani dowód odporności na mutację guarda.
`exercise_handler` sprawdza dostarczoną odpowiedź wobec dostarczonego schematu.
Nie ma podstaw do deklaracji generatora dozwolonych/zabronionych impulsów,
redukcji kluczy impulsu, redukcji sesji ani globalnej minimalności.
Dla odroczonych mechanizmów nie istnieją potwierdzone pary command/event.

## Wykonane próby kontroli

Host: `mini-m4-0.localdomain`, `Darwin arm64`.
Odczytane wersje hosta: mise `2026.9.12`, pixi `0.81.0`.
`mise.toml` wymaga pixi `0.72.2` i uv `0.11.2`; `pixi.toml` wymaga Mojo `1.1.0`.
Wersji uruchomionego środowiska i Mojo nie potwierdzono.

Podjęto dziesięć prób: full-smoke przed zmianą i po zmianie dokumentu oraz osiem kontroli poniżej.
Kod produkcyjny pozostał identyczny z początkowym SHA; zmienił się wyłącznie ten dokument.
Każda używała `sandbox-exec` z zakazem zapisu poza bieżącym checkoutem,
lokalnym TMPDIR i lokalnymi katalogami cache, a następnie `mise exec -- pixi run <zadanie>`.
Każda zakończyła się kodem **71**: `sandbox_apply: Operation not permitted`.
Izolacja nie uruchomiła narzędzi ani sprawdzianów; nie obchodzono jej ograniczeń.

| Zadanie | Wynik |
| --- | --- |
| `full-smoke` przed zmianą i po zmianie | Obie próby zablokowane przed startem |
| `exerciser` | Zablokowane przed startem; dodatkowo brak zadania w pixi.toml |
| `output-contracts` | Zablokowane przed startem |
| `durable-subprocess` | Zablokowane przed startem |
| `run-identity-replay` | Zablokowane przed startem |
| `runtime-contracts` | Zablokowane przed startem |
| `native-subprocess` | Zablokowane przed startem |
| `native-driver` | Zablokowane przed startem |
| `sqlite-journal-port` | Zablokowane przed startem |

Licznik tych prób: 10; wykonane zestawy: 0; zaliczone: 0; niezaliczone zestawy: 0; zablokowane: 10.
`full-smoke` obejmuje `core-smoke` i `adapter-smoke`, nie osobny `runtime-contracts`.
Sprawdziany mają stałe ścieżki `/tmp`; samo ustawienie TMPDIR nie zapewnia wymaganej izolacji.
Nie wykazano usterki produkcyjnej uzasadniającej zmianę runtime lub osłabienie kontroli.

`.github/workflows/linux-arm64.yml` definiuje kontrolę `Linux ARM64 full smoke`
na `ubuntu-24.04-arm`: `mise exec -- pixi run full-smoke`.
Nie potwierdzono konfiguracji wymaganych statusów ani wyniku CI końcowego SHA.
Brak dostępu do izolowanego wykonania oraz brak dowodu Linux ARM64 blokują odbiór.
