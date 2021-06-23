#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "comun.h"
#include "diccionario.h"
#include "cola.h"
#include <stdint.h>



#include <netdb.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/uio.h>

#define SIZE 1024

void error_read(int socketServ, int socketClient);
void print_colas(char *c, void *v);
void print_mensajes(void *v);
void liberar_cola(char *c, void *v);
void liberar_mensajes(void *v);

int error, error2;

int main(int argc, char *argv[])
{
    int socketServ, socketClient, leido;
    unsigned int tam_dir;
    struct sockaddr_in dir, dir_cliente;
    struct iovec iov[3];
    char *msg, *cola;
    int *tam = malloc(sizeof(int));
    char *op = malloc(sizeof(char));
    int opcion = 1;
    int ok = 1;
    int fail = 0;

    fprintf(stdout, "se inicia la ejecucion\n");
    // comprobacion numero de parametros
    if (argc != 2) {
        fprintf(stderr, "Uso: %s puerto\n", argv[0]);
        return 1;
    }

    // se crea el socket
    if ((socketServ = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return 1;
    }

    // Para reutilizar puerto inmediatamente
    if (setsockopt(socketServ, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion)) < 0) {
        perror("error en setsockopt");
        return 1;
    }

    // Se inicializan los valores del struct dir
    dir.sin_addr.s_addr = INADDR_ANY;
    dir.sin_port = htons(atoi(argv[1]));
    dir.sin_family = PF_INET;

    // 
    if (bind(socketServ, (struct sockaddr *)&dir, sizeof(dir)) < 0)
    {
        perror("error en bind");
        close(socketServ);
        return 1;
    }

    // 
    if (listen(socketServ, 10) < 0) {
        perror("error en listen");
        close(socketServ);
        return 1;
    }

    // Inicia estructuras de datos de las colas
    struct diccionario *d;
    d = dic_create();

    while (1){
        tam_dir = sizeof(dir_cliente);
        if ((socketClient = accept(socketServ, (struct sockaddr *)&dir_cliente, &tam_dir)) < 0) {
            perror("error en accept");
            close(socketServ);
            return 1;
        }

        fprintf(stdout, "se acepta una peticion\n");
        if ((leido=read(socketClient, op, sizeof(char))) < 0) {
            error_read(socketServ, socketClient);
			return 1;
		}

        // createMQ
        if(*op == 'C'){        

            fprintf(stdout, "Se procede a crear una nueva cola\n");                
            if((leido=read(socketClient, tam, sizeof(int))) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            cola = malloc(*tam);
            if((leido=read(socketClient, cola, *tam)) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            if (dic_put(d, cola, cola_create()) < 0){
                iov[0].iov_base = &fail; 
                iov[0].iov_len = sizeof(int);
            } else {                
                iov[0].iov_base = &ok; 
                iov[0].iov_len = sizeof(int);
            }            
            writev(socketClient, iov, 1);
        }

        // destroyMQ
        else if(*op == 'D'){
            fprintf(stdout, "Se procede a destruir una cola\n");

            if((leido=read(socketClient, tam, sizeof(int))) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            cola = malloc(*tam);
            if((leido=read(socketClient, cola, *tam)) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }
            
            if(dic_remove_entry(d, cola, liberar_cola) < 0){
                iov[0].iov_base = &fail; 
                iov[0].iov_len = sizeof(int);
            } else if (error < 0) {
                iov[0].iov_base = &fail; 
                iov[0].iov_len = sizeof(int);
            } else {
                iov[0].iov_base = &ok; 
                iov[0].iov_len = sizeof(int);                
            }        
            writev(socketClient, iov, 1);
        }

        // put
        else if(*op == 'P'){
            fprintf(stdout, "Se procede introducir un nuevo mensaje\n");

            if((leido=read(socketClient, tam, sizeof(int))) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            cola = malloc(*tam);
            if((leido=read(socketClient, cola, *tam)) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            if((leido=read(socketClient, tam, sizeof(int))) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            msg = malloc(*tam);
            if((leido=read(socketClient, msg, *tam)) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            if(cola_push_back(dic_get(d, cola, &error),msg) < 0){
                iov[0].iov_base = &fail; 
                iov[0].iov_len = sizeof(int);
            } else {
                iov[0].iov_base = &ok; 
                iov[0].iov_len = sizeof(int); 
            }
            writev(socketClient, iov, 1);
        }

        // get
        else if(*op == 'G'){
            fprintf(stdout, "Se procede a extraer un mensaje\n");

            if((leido=read(socketClient, tam, sizeof(int))) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            cola = malloc(*tam);
            if((leido=read(socketClient, cola, *tam)) < 0 ) {
                error_read(socketServ, socketClient);
                return 1;
            }

            msg = cola_pop_front(dic_get(d, cola, &error), &error2);
            if(error < 0 || error2 < 0){
                iov[0].iov_base = &fail; 
                iov[0].iov_len = sizeof(int);
                writev(socketClient, iov, 1);
            } else {
                int tamMsg = strlen(msg);
                iov[0].iov_base = &ok; 
                iov[0].iov_len = sizeof(int);
                iov[1].iov_base = &tamMsg; 
                iov[1].iov_len = sizeof(int); 
                iov[2].iov_base = msg; 
                iov[2].iov_len = strlen(msg); 
                writev(socketClient, iov, 3);
            }
        }

        dic_visit(d, print_colas);
        close(socketClient);
    }
    close(socketServ);
    return 0;
}

void print_colas(char *c, void *v){
    struct cola *cola = v;
    printf("%s --->", c);
    cola_visit(cola, print_mensajes);
    printf("n");
}

void print_mensajes(void *v){
    char *msg = v;
    printf(" %s", msg);
}

void liberar_cola(char *c, void *v){
	struct cola *cola = v;
	free(c);
	error = cola_destroy(cola, liberar_mensajes);
}

void liberar_mensajes(void *v){
    free(v);
}

void error_read(int socketServ, int socketClient){
    perror("error en read");
    close(socketServ);
    close(socketClient);
}