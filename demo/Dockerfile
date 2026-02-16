FROM nginx:alpine
RUN rm -rf /usr/share/nginx/html/*
RUN mkdir -p /etc/nginx/templates
COPY css/ /usr/share/nginx/html/css/
COPY js/ /usr/share/nginx/html/js/
COPY data/ /usr/share/nginx/html/data/
COPY index.html /usr/share/nginx/html/
RUN printf 'server {\n    listen ${PORT};\n    server_name _;\n    root /usr/share/nginx/html;\n    index index.html;\n    location / {\n        try_files $$uri $$uri/ /index.html;\n    }\n}\n' > /etc/nginx/templates/default.conf.template
EXPOSE 3005
