class APIError {
  public error: any | null = null
  constructor({error}: { error: any }) {
    this.error = error
  }
}

export default APIError
