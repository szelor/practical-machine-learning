library(gapminder)
library(tidyverse)
library(modelr)

dim(gapminder)
str(gapminder)
gapminder

ggplot(gapminder, aes(year, lifeExp)) +
  geom_line()

ggplot(gapminder, aes(year, lifeExp, group = country)) +
  geom_line()

#focus on a single country
pl <- filter (gapminder, country =="Poland")

ggplot(pl, aes(year, lifeExp)) +
  geom_point(size = 2.5)

#that's our goal
ggplot(pl, aes(year, lifeExp)) +
  geom_point(size = 2) +
  geom_smooth(method = "lm", size = 1.5, colour = "red")

#random models
models <- tibble( 
  a1 = runif(5000, -400, 400), 
  a2 = runif(5000, -1, 1) ) 

ggplot(pl, aes(year, lifeExp)) + 
  geom_abline( aes( intercept = a1, slope = a2),
               data = models, alpha = 1/2 ) + 
  geom_point(size = 2)

#linear model
model1 <- function(a,data) {
  a[1] +data$year * a[2]
}

model1(c(-300,0.2), pl)

#prediction errors
errors <- function (mod,data) {
  diff <- data$lifeExp - model1(mod,data)
  sqrt(mean(diff^2))
}
errors(c(-300,0.2), pl)

#use purrr to compute errors for all radom models
pl_dist <- function(a1,a2) {
  errors(c(a1,a2),pl)
}

models <- models %>%
  mutate(dist = map2_dbl(a1,a2,pl_dist))
models

ggplot(pl, aes(year, lifeExp)) + 
  geom_point(size = 2) +
  geom_abline(aes(intercept = a1, slope = a2, color =-dist), size = 1.1,
              data = filter(models,  rank(dist) <20) )


#grid search
ggplot(models, aes(a1,a2)) +
  geom_point(data = filter(models,  rank(dist) <20),
             size=4, color = "red",  fill="red" ) +
  geom_point(aes(color =-dist), alpha = 1/4)

grid <- expand.grid(
  a1 = seq(-400, 400, length = 30), 
  a2 = seq(-0.35, 0.35, length = 30) 
) %>% 
  mutate(dist = map2_dbl( a1, a2, pl_dist)) 

grid %>% 
  ggplot( aes(a1, a2)) + 
  geom_point(data = filter( grid, rank( dist) <20), 
              size = 4, colour = "red" ) + 
  geom_point(aes( color = -dist))

ggplot(pl, aes(year, lifeExp)) + 
  geom_point(size = 2, color = "grey30") +
  geom_abline(aes(intercept = a1, slope = a2, color =-dist),size = 1.1,
              data = filter(grid,  rank(dist) <20) )

#Newton-Raphson search
best <- optim(c(0,0), errors, data = pl)
best$par

ggplot(pl, aes(year, lifeExp)) + 
  geom_point(size = 2, color = "grey30") +
  geom_abline(intercept = best$par[1], slope = best$par[2], color = "red", size = 1.1)

#fitting a linear model with lm()
mod <- lm(formula = lifeExp ~ year, data = pl)
summary(mod)
coef(summary(mod))

# Visualizing a model

#create a grid
grid_pl <- pl %>%
  data_grid(year)
grid_pl

#add predictions 
grid_pl <- grid_pl %>%
  add_predictions(mod)
grid_pl

ggplot(pl, aes(year, lifeExp)) +
  geom_point(size = 2) +
  geom_line(aes(y=pred), data = grid_pl, color = "red", size = 1.5)


#residuals
pl <- pl %>%
  add_residuals(mod)
pl

ggplot(pl, aes(year,resid)) +
  geom_point(size = 2)

#fitting another linear model with lm()
mod2 <- lm(lifeExp ~ year + pop + gdpPercap, data = pl)
summary(mod2)
coef(summary(mod2))

pl <- pl %>%
  add_residuals(mod2, var = "resid2")
pl


