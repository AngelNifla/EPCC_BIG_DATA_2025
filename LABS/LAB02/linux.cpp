
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <set>
#include <omp.h>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

std::vector<std::string> leerPalabrasBusqueda(const std::string& archivo) {
    std::vector<std::string> palabras;
    std::ifstream in(archivo);
    std::string palabra;
    while (in >> palabra) {
        palabras.push_back(palabra);
    }
    return palabras;
}

std::string leerArchivo(const std::string& ruta) {
    int fd = open(ruta.c_str(), O_RDONLY);
    if (fd < 0) return "";
    struct stat sb;
    if (fstat(fd, &sb) == -1) return "";
    size_t length = sb.st_size;
    char* data = (char*) mmap(NULL, length, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) return "";
    std::string contenido(data, length);
    munmap(data, length);
    close(fd);
    return contenido;
}

int main() {
    std::vector<std::string> palabrasBusqueda = leerPalabrasBusqueda("words1.txt");
    std::vector<std::string> archivos = {
        "Data/1GB_1.txt", "Data/1GB_2.txt", "Data/1GB_3.txt", "Data/1GB_4.txt",
        "Data/1GB_5.txt", "Data/1GB_6.txt", "Data/1GB_7.txt", "Data/1GB_8.txt",
        "Data/1GB_9.txt", "Data/1GB_10.txt", "Data/1GB_11.txt", "Data/1GB_12.txt",
        "Data/1GB_13.txt", "Data/1GB_14.txt", "Data/1GB_15.txt", "Data/1GB_16.txt",
        "Data/1GB_17.txt", "Data/1GB_18.txt", "Data/1GB_19.txt", "Data/1GB_20.txt"
    };

    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::string, std::set<std::string>> resultadoGlobal;

    #pragma omp parallel num_threads(10)
    {
        std::map<std::string, std::set<std::string>> resultadoLocal;

        #pragma omp for schedule(dynamic, 1) nowait
        for (int i = 0; i < archivos.size(); ++i) {
            const std::string& archivo = archivos[i];
            std::string contenido = leerArchivo(archivo);

            for (const auto& palabra : palabrasBusqueda) {
                if (contenido.find(palabra) != std::string::npos) {
                    resultadoLocal[palabra].insert(archivo);
                }
            }
        }

        #pragma omp critical
        {
            for (const auto& par : resultadoLocal) {
                resultadoGlobal[par.first].insert(par.second.begin(), par.second.end());
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Tiempo: " << dur.count() / 1000 << "s " << dur.count() % 1000 << "ms\n";

    std::ofstream out("resultados.txt");
    for (const auto& par : resultadoGlobal) {
        out << par.first << ": \t";
        for (const auto& a : par.second) out << a[9] << a[10] << " / ";
        out << "\n";
    }
    out << "\nTiempo total: " << dur.count() / 1000 << "s " << dur.count() % 1000 << "ms\n";
    out.close();
    return 0;
}
