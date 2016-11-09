namespace scala authService.rpc

service authService {

  string authenticate(1: string login, 2: string password),

  bool authorize(1: string token)
}