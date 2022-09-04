library(caret)

myData = read.delim("./prediccion.txt", header = FALSE, sep=',')

myData$V1 <- as.factor(myData$V1) #The date is a factor

#Normalize the average
a = summary(myData$V146)
max = a[6]

for (i in 1: nrow(myData))
{
  myData[i, ]$V146 = myData[i, ]$V146 / max
}

#We just catch the 30% to test
inTrain <- createDataPartition(y = myData$V146, p = .70, list = FALSE)

training <- myData[ inTrain,]
testing <- myData[-inTrain,]

nnetData = train(V146 ~ ., data = training, method="nnet",verbose = FALSE)

#The summary of the average
summary(myData$V146) * max

#Prediction
pred <-predict(nnetData,newdata=testing) * max

obs <- testing$V146 * max

plot(pred, obs, main = "Observado frente a predicciones",
     xlab = "Predicción", ylab = "Observado")

accuracy <- function(pred, obs, na.rm = FALSE,
                     tol = sqrt(.Machine$double.eps)) {
  err <- obs - pred     # Errores
  if(na.rm) {
    is.a <- !is.na(err)
    err <- err[is.a]
    obs <- obs[is.a]
  }
  perr <- 100*err/pmax(obs, tol)  # Errores porcentuales
  return(c(
    me = mean(err),           # Error medio
    rmse = sqrt(mean(err^2)), # Raíz del error cuadrático medio
    mae = mean(abs(err)),     # Error absoluto medio
    mpe = mean(perr),         # Error porcentual medio
    mape = mean(abs(perr)),   # Error porcentual absoluto medio
    r.squared = 1 - sum(err^2)/sum((obs - mean(obs))^2) # Pseudo R-cuadrado
  ))
}

accuracy(pred, obs)
