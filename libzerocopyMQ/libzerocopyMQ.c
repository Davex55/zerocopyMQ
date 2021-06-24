#include <stdint.h>
#include "zerocopyMQ.h"
#include "comun.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <limits.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/uio.h>


#define SIZE 1024
#define TAM 1024

int createSocket();

int createMQ(const char *cola) {
    int socketServ, tamCola, leido;
    struct iovec iov[3];
    int *result = malloc(sizeof(int));
    char c = 'C';
    
    if((socketServ = createSocket()) == 1 ){
        return -1;
    }       

    if((tamCola = strlen(cola) + 1 ) > SIZE){
        perror("El nombre de la cola es demasiado grande");
        return -1;
    }

    iov[0].iov_base = &c; 
    iov[0].iov_len = sizeof(c);
    iov[1].iov_base = &tamCola;
    iov[1].iov_len = sizeof(tamCola);
    iov[2].iov_base = (char *)cola;
    iov[2].iov_len = strlen(cola) + 1;
    
    writev(socketServ, iov, 3);

    if ((leido = read(socketServ, result, sizeof(int))) < 0) {
        perror("error en read");
        close(socketServ);
        return -1;
    }

    close(socketServ);

    if(*result){
        free(result);
        return 0;
    } else {
        free(result);
        return -1;
    }
}

int destroyMQ(const char *cola){
    int socketServ, tamCola, leido;
    struct iovec iov[3];
    int *result = malloc(sizeof(int));
    char c = 'D';
    
    if((socketServ = createSocket()) == 1 ){
        return -1;
    }       

    if((tamCola = strlen(cola) + 1 ) > SIZE){
        perror("El nombre de la cola es demasiado grande");
        return -1;
    }

    iov[0].iov_base = &c; 
    iov[0].iov_len = sizeof(c);
    iov[1].iov_base = &tamCola;
    iov[1].iov_len = sizeof(tamCola);
    iov[2].iov_base = (char *)cola;
    iov[2].iov_len = strlen(cola) + 1;
    
    writev(socketServ, iov, 3);

    if ((leido = read(socketServ, result, sizeof(int))) < 0) {
        perror("error en read");
        close(socketServ);
        return -1;
    }

    close(socketServ);

    if(*result){
        free(result);    
        return 0;
    } else {
        free(result);
        return -1;
    }      
}

int put(const char *cola, const void *mensaje, uint32_t tam) {
    int socketServ, tamCola, leido;
    struct iovec iov[5];
    int *result = malloc(sizeof(int));
    char c = 'P';
    
    if((socketServ = createSocket()) == 1 ){
        return -1;
    }       

    if((tamCola = strlen(cola) + 1 ) > SIZE){
        perror("El nombre de la cola es demasiado grande");
        return -1;
    }

    if(tam == 0){
        fprintf(stdout, "El mensaje esta vacio\n");
        return 0;
    }

    if(tam > SIZE){
        perror("El mensaje es demasiado grande");
        return -1;
    }

    iov[0].iov_base = &c; 
    iov[0].iov_len = sizeof(c);
    iov[1].iov_base = &tamCola;
    iov[1].iov_len = sizeof(tamCola);
    iov[2].iov_base = (char *)cola;
    iov[2].iov_len = strlen(cola) + 1;
    iov[3].iov_base = &tam;
    iov[3].iov_len = sizeof(tam);
    iov[4].iov_base = (char *)mensaje;
    iov[4].iov_len = tam;
    
    writev(socketServ, iov, 5);

    if ((leido = read(socketServ, result, sizeof(int))) < 0) {
        perror("error en read");
        close(socketServ);
        return -1;
    }

    close(socketServ);

    if(*result){
        free(result);      
        return 0;
    } else {
        free(result);
        return -1;
    }      
}

int get(const char *cola, void **mensaje, uint32_t *tam, bool blocking) {
    int socketServ, tamCola, leido;
    struct iovec iov[3];
    int *result = malloc(sizeof(int));
    char c = 'G';
    
    if((socketServ = createSocket()) == 1 ){
        return -1;
    }       

    if((tamCola = strlen(cola) + 1 ) > SIZE){
        perror("El nombre de la cola es demasiado grande");
        return -1;
    }

    iov[0].iov_base = &c; 
    iov[0].iov_len = sizeof(c);
    iov[1].iov_base = &tamCola;
    iov[1].iov_len = sizeof(tamCola);
    iov[2].iov_base = (char *)cola;
    iov[2].iov_len = strlen(cola) + 1;

    writev(socketServ, iov, 3);

    if ((leido = read(socketServ, result, sizeof(int))) < 0) {
        perror("error en read");
        close(socketServ);
        return -1;
    }
    
    if(!*result){
        free(result);
        return -1;
    } 

    if ((leido = read(socketServ, tam, sizeof(uint32_t))) < 0) {
        perror("error en read");
        close(socketServ);
        return -1;
    }
    
    if(*tam == 0){
        close(socketServ);
        free(result);
        return 0;
    }

    *mensaje = malloc(*tam);
    if((leido=read(socketServ, *mensaje, *tam)) < 0 ) {
        perror("error en read");
        close(socketServ);
        return -1;
    }

    close(socketServ);
    free(result);
    return 0;
}

int createSocket(){
    int socketServ;
	struct sockaddr_in dir;
	struct hostent *host_info;
    char *broker_host, *broker_port;

    broker_host = getenv("BROKER_HOST");
    broker_port = getenv("BROKER_PORT");

	host_info = gethostbyname(broker_host);
	dir.sin_addr=*(struct in_addr *)host_info->h_addr;
	dir.sin_port=htons(atoi(broker_port));
	dir.sin_family=PF_INET;

	if ((socketServ=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		perror("error creando socket");
		return 1;
	}
	
    if (connect(socketServ, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
		perror("error en connect");
		close(socketServ);
		return 1;
	}    

    return socketServ;
}

// int main(int argc, char *argv[])
// {
//     char *c;
//     c = "test";    
//     createMQ(c);
//     return 0;
// }

