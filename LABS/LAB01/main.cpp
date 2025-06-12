#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include <chrono>

using namespace std;

using WordCount = unordered_map<string, int>;   //Alias para un mapa no ordenado que asocia cadenas con enteros (conteo de cada palabra).

/* 
//////////////////////////METODO DONDE EL hola y Hola se suma como 1 y no como 2.
#include <cctype>  // isalpha, tolower
#include <algorithm>

// Función auxiliar para limpiar palabra
string normalize(const string& input) {
    string result;
    for (char c : input) {
        if (isalpha(static_cast<unsigned char>(c))) {
            result += tolower(static_cast<unsigned char>(c));
        }
        // Opcional: puedes agregar reglas para reemplazar caracteres con tilde si deseas
    }
    return result;
}

void process_chunk(const string& data, WordCount& local_map) {
    istringstream iss(data);
    string word;
    while (iss >> word) {
        string normalized = normalize(word);
        if (!normalized.empty())
            local_map[normalized]++;
    }
}

*/

//Funcion que cuenta cuántas veces aparece cada palabra dentro de un Fragmento de texto.
void process_chunk(const string& palabra, WordCount& local_map) {
    istringstream leer(palabra);
    string word;
    while (leer >> word) {
        local_map[word]++;
    }
}

//Funcion que combina los resultados parciales de conteo de palabras en un solo mapa final.
void merge_maps(const vector<WordCount>& MapasParciales, WordCount& MapaFinal) {
    for (const auto& MapaLocal : MapasParciales) {
        for (const auto& [word, count] : MapaLocal) {
            MapaFinal[word] += count;
        }
    }
}

int main() {
    const int num_threads = 6;
    const size_t SizeFragmento = 512 * 1024 * 1024;  // 512 MB
    const size_t SizeFragmentoExtra = 1024; // 1 KB de superposición

    //ifstream ArchivoEntrada("500MB.txt", ios::binary);
    //ifstream ArchivoEntrada("5GB.txt", ios::binary);
    ifstream ArchivoEntrada("20_2GB.txt", ios::binary);
    if (!ArchivoEntrada) {
        cerr << "No se pudo abrir el archivo." << endl;
        return 1;
    }

    auto timeInicio = chrono::steady_clock::now();

    WordCount Mapa_Final;
    vector<char> buffer(SizeFragmento + SizeFragmentoExtra);
    string ParteSobranteFragmento;  // Parte sobrante del bloque anterior

    while (ArchivoEntrada) {
        vector<thread> threads;//Vector para almacenar los hilos que se crearán.
        vector<string> Vec_Fragmentos;//Segmentos de texto que se asignarán a cada hilo.
        vector<WordCount> MapasParciales;//Mapas parciales para almacenar el conteo de palabras de cada hilo.

        for (int i = 0; i < num_threads && ArchivoEntrada; ++i) {
            ArchivoEntrada.read(buffer.data(), SizeFragmento + SizeFragmentoExtra);
            streamsize bytes_read = ArchivoEntrada.gcount();
            if (bytes_read <= 0) break;

            string data(buffer.data(), bytes_read);

            // Cortar en la última posición segura
            size_t cut_pos = data.find_last_of(" \t\n\r.,;:!?\"()[]{}<>");

            if (cut_pos == string::npos || cut_pos == 0)
                cut_pos = bytes_read; // No se encontró separador, procesar todo

            // La parte válida para procesar ahora
            string valid_chunk = ParteSobranteFragmento + data.substr(0, cut_pos);
            ParteSobranteFragmento = data.substr(cut_pos); // Guardar para el siguiente bloque

            Vec_Fragmentos.push_back(move(valid_chunk));
            MapasParciales.emplace_back();
        }

        if (Vec_Fragmentos.empty()) break;

        // Lanzar hilos
        for (size_t i = 0; i < Vec_Fragmentos.size(); ++i) {
            threads.emplace_back(process_chunk, cref(Vec_Fragmentos[i]), ref(MapasParciales[i]));
        }

        for (auto& t : threads) t.join();//Se espera a que todos los hilos terminen.

        merge_maps(MapasParciales, Mapa_Final);//Se combinan los mapas parciales en el mapa final.
    }

    // Procesar el sobrante final si hay:  se procesa para asegurar que no se pierda ninguna palabra.
    if (!ParteSobranteFragmento.empty()) {
        WordCount last_map;
        process_chunk(ParteSobranteFragmento, last_map);
        merge_maps({last_map}, Mapa_Final);
    }

    ArchivoEntrada.close();

    auto timeSalida = chrono::steady_clock::now();
    auto timeTranscurrido = chrono::duration_cast<chrono::seconds>(timeSalida - timeInicio);

    // Ordena: Convierte el mapa final en un vector(palabra, frecuencia).Y lo en ordena descendente según la frecuencia.
    vector<pair<string, int>> PalabrasOrdenadas(Mapa_Final.begin(), Mapa_Final.end());
    sort(PalabrasOrdenadas.begin(), PalabrasOrdenadas.end(), [](auto& a, auto& b) {
        return b.second < a.second;
    });

    // Resultados
    ofstream ArchivoSalida("output.txt");
    if (!ArchivoSalida) {
        cerr << "No se pudo escribir el archivo de salida." << endl;
        return 1;
    }

    size_t TotalPalabras = 0;
    for (const auto& [word, count] : PalabrasOrdenadas) {
        TotalPalabras += count;
    }

    ArchivoSalida << "Total de palabras revisadas: " << TotalPalabras << "\n";
    ArchivoSalida << "Total de palabras distintas: " << PalabrasOrdenadas.size() << "\n\n";
    for (const auto& [word, count] : PalabrasOrdenadas) {
        ArchivoSalida << word << ": " << count << "\n";
    }

    cout << "Tiempo: " << timeTranscurrido.count() << " segundos\n";
    //cout << "Total palabras: " << TotalPalabras << "\n";
    cout << "Palabras distintas: " << PalabrasOrdenadas.size() << "\n";
    cout << "FIN\n";

    return 0;
}