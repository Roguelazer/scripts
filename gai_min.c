#include<netdb.h>
#include<arpa/inet.h>
main(int c,char**v){struct addrinfo h={.ai_socktype=1},*r;getaddrinfo(v[1],0,&h,&r);puts(inet_ntoa(((struct sockaddr_in*)r->ai_addr)->sin_addr));}
