namespace scala authService.rpc

service authService {

  string authenticate(1: string login, 2: string password),

  bool isValid(1: string token)
}