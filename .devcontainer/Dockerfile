# change source image if necessary
# see 
FROM ghcr.io/rocker-org/devcontainer/tidyverse:4.3

# key dependencies for utilities
RUN apt-get update -qq \
  && export DEBIAN_FRONTEND=noninteractive \
  && apt-get -y install --no-install-recommends libxt6 curl libglpk-dev libcurl4-openssl-dev libssl-dev libxml2-dev libarchive-dev

# install rpython and pip3 just in case
RUN apt-get update \
  && export DEBIAN_FRONTEND=noninteractive \
  && apt-get -y install --no-install-recommends python3-setuptools python3-pip

# Optional: add shinycannon to the container if load-testing shiny apps with shinyloadtest package
#RUN curl -L https://github.com/rstudio/shinycannon/releases/download/v1.1.3/shinycannon_1.1.3-dd43f6b_amd64.deb -o /tmp/shinycannon.deb
#RUN apt-get update \
#  && apt-get -y install --no-install-recommends default-jre-headless \
#  && apt-get -y install ./tmp/shinycannon.deb

# Optional: add headless chrome to the container if using shinytest2 to test shiny apps
#RUN curl -L https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb -o /tmp/chrome.deb
#RUN apt-get update -qq \
#  && apt-get -y install ./tmp/chrome.deb

# install R package dependencies
# add / remove packages from line below (but keep httpgd, renv, languageserver
RUN install2.r renv httpgd languageserver remotes dplyr dbplyr DBI RSQLite purrr stringr ggplot2 reactable reactablefmtr quarto rsconnect pointblank gt stringdist tidystringdist tictoc reclin2 fastLink anytime duckplyr epoxy archive

# install python packages
RUN pip3 install splink jupyter altair pandas siuba

#RUN installGithub.r gadenbuie/xaringanExtra

